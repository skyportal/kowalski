import datetime
import os
import pathlib
import requests
import subprocess
import time
from typing import List, Optional

from kowalski.alert_brokers.alert_broker_ztf import watchdog
from kowalski.ingesters.ingester import KafkaStream
from kowalski.utils import init_db_sync, Mongo
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

USING_DOCKER = os.environ.get("USING_DOCKER", False)
if USING_DOCKER:
    config["server"]["host"] = "kowalski_api_1"


class Program:
    def __init__(
        self,
        group_name: str = "FRITZ_TEST",
        group_nickname: str = "Fritz",
        stream_ids: Optional[List[int]] = None,
        filter_name: str = "Orange Transients",
    ):
        self.access_token = config["skyportal"]["token"]
        self.base_url = (
            f"{config['skyportal']['protocol']}://"
            f"{config['skyportal']['host']}:{config['skyportal']['port']}"
        )
        self.headers = {"Authorization": f"token {self.access_token}"}

        self.group_name = group_name
        self.group_nickname = group_nickname
        self.group_id, self.filter_id = self.create(
            stream_ids=stream_ids, filter_name=filter_name
        )

    def get_groups(self):
        resp = requests.get(
            self.base_url + "/api/groups",
            headers=self.headers,
            timeout=3,
        )

        assert resp.status_code == requests.codes.ok
        result = resp.json()

        assert result["status"] == "success"
        assert "data" in result
        assert "user_groups" in result["data"]

        user_groups = {
            g["name"]: g["id"] for g in result["data"]["user_accessible_groups"]
        }

        return user_groups

    def get_group_filters(self, group_id: int):
        resp = requests.get(
            self.base_url + "/api/filters",
            headers=self.headers,
            timeout=3,
        )

        assert resp.status_code == requests.codes.ok
        result = resp.json()

        assert result["status"] == "success"
        assert "data" in result

        group_filter_ids = [
            fi["id"] for fi in result["data"] if fi["group_id"] == group_id
        ]

        return group_filter_ids

    def create(
        self,
        stream_ids: Optional[List[int]] = None,
        filter_name: str = "Orange Transients",
    ):
        user_groups = self.get_groups()

        if stream_ids is None:
            stream_ids = [1, 2]

        if self.group_name in user_groups.keys():
            # already exists? grab its id then:
            group_id = user_groups[self.group_name]
        else:
            # else, create a new group and add stream access to it
            resp = requests.post(
                self.base_url + "/api/groups",
                json={"name": self.group_name, "nickname": self.group_nickname},
                headers=self.headers,
                timeout=3,
            )
            result = resp.json()

            assert result["status"] == "success"
            assert "data" in result
            assert "id" in result["data"]

            group_id = result["data"]["id"]

            # grant stream access to group
            for stream_id in stream_ids:
                resp = requests.post(
                    self.base_url + f"/api/groups/{group_id}/streams",
                    json={"stream_id": stream_id},
                    headers=self.headers,
                    timeout=3,
                )
                result = resp.json()

                assert result["status"] == "success"
                assert result["data"]["stream_id"] == stream_id

        # grab filter_ids defined for this group:
        group_filter_ids = self.get_group_filters(group_id=group_id)

        if len(group_filter_ids) == 0:
            # none created so far? make one:
            resp = requests.post(
                self.base_url + "/api/filters",
                json={
                    "name": filter_name,
                    "stream_id": max(stream_ids),
                    "group_id": group_id,
                },
                headers=self.headers,
                timeout=3,
            )
            result = resp.json()

            assert result["status"] == "success"
            assert "data" in result
            assert "id" in result["data"]

            filter_id = result["data"]["id"]
        else:
            # else just grab the first one
            filter_id = group_filter_ids[0]

        return group_id, filter_id

    def remove(self):
        user_groups = self.get_groups()

        group_filter_ids = self.get_group_filters(group_id=self.group_id)
        for filter_id in group_filter_ids:
            resp = requests.delete(
                self.base_url + f"/api/filters/{filter_id}",
                headers=self.headers,
                timeout=3,
            )
            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"

        if self.group_name in user_groups.keys():

            resp = requests.delete(
                self.base_url + f"/api/groups/{user_groups[self.group_name]}",
                headers=self.headers,
                timeout=3,
            )
            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"

        self.group_id, self.filter_id = None, None


class Filter:
    def __init__(
        self,
        collection: str = "ZTF_alerts",
        group_id=None,
        filter_id=None,
        permissions=None,
        autosave: bool = False,
        update_annotations: bool = False,
        pipeline=None,
    ):
        assert group_id is not None
        assert filter_id is not None

        self.access_token = self.get_api_token()
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        self.collection = collection
        self.group_id = int(group_id)
        self.filter_id = int(filter_id)
        self.autosave = autosave
        self.update_annotations = update_annotations
        self.permissions = permissions if permissions is not None else [1, 2]

        self.pipeline = pipeline
        if self.pipeline is None:
            self.pipeline = [
                {
                    "$match": {
                        "candidate.drb": {"$gt": 0.9},
                    }
                },
                {
                    "$addFields": {
                        "annotations.author": "dd",
                        "annotations.mean_rb": {"$avg": "$prv_candidates.rb"},
                    }
                },
                {"$project": {"_id": 0, "candid": 1, "objectId": 1, "annotations": 1}},
            ]

        self.fid = self.create()

    @staticmethod
    def get_api_token():
        a = requests.post(
            f"http://{config['server']['host']}:{config['server']['port']}/api/auth",
            json={
                "username": config["server"]["admin_username"],
                "password": config["server"]["admin_password"],
            },
        )
        credentials = a.json()
        token = credentials.get("token", None)

        return token

    def create(self):

        user_filter = {
            "group_id": self.group_id,
            "filter_id": self.filter_id,
            "catalog": self.collection,
            "permissions": self.permissions,
            "autosave": self.autosave,
            "update_annotations": self.update_annotations,
            "pipeline": self.pipeline,
        }

        # save:
        resp = requests.post(
            f"http://{config['server']['host']}:{config['server']['port']}/api/filters",
            json=user_filter,
            headers=self.headers,
            timeout=5,
        )
        assert resp.status_code == requests.codes.ok
        result = resp.json()
        assert result["status"] == "success"
        assert "data" in result
        assert "fid" in result["data"]
        fid = result["data"]["fid"]
        return fid

    def remove(self):
        resp = requests.delete(
            f"http://{config['server']['host']}:{config['server']['port']}/api/filters",
            json={"group_id": self.group_id, "filter_id": self.filter_id},
            headers=self.headers,
            timeout=5,
        )
        assert resp.status_code == requests.codes.ok
        result = resp.json()
        assert result["status"] == "success"
        assert (
            result["message"]
            == f"removed filter for group_id={self.group_id}, filter_id={self.filter_id}"
        )

        self.fid = None


class TestIngester:
    """
    End-to-end ingester test:
        - Create test program in Fritz
        - Spin up ZooKeeper
        - Spin up Kafka server
        - Create topic
        - Publish test alerts to topic
        - Create test filter
        - Spin up ingester
        - Digest and ingest alert stream, post to Fritz
        - Delete test filter
    """

    def test_ingester(self):

        init_db_sync(config=config, verbose=True)

        log("Setting up paths")

        path_logs = pathlib.Path("logs/")
        if not path_logs.exists():
            path_logs.mkdir(parents=True, exist_ok=True)

        log("Checking the existing ZTF alert collection states")
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            srv=config["database"]["srv"],
            verbose=True,
        )
        collection_alerts = config["database"]["collections"]["alerts_ztf"]
        collection_alerts_aux = config["database"]["collections"]["alerts_ztf_aux"]

        # check if the collection exists, drop it if it does
        if collection_alerts in mongo.db.list_collection_names():
            try:
                mongo.db[collection_alerts].drop()
            except Exception as e:
                log(f"Failed to drop the collection {collection_alerts}: {e}")
        if collection_alerts_aux in mongo.db.list_collection_names():
            try:
                mongo.db[collection_alerts_aux].drop()
            except Exception as e:
                log(f"Failed to drop the collection {collection_alerts_aux}: {e}")

        if config["misc"]["broker"]:
            log("Setting up test groups and filters in Fritz")
            program = Program(group_name="FRITZ_TEST", group_nickname="test")
            Filter(
                collection="ZTF_alerts",
                group_id=program.group_id,
                filter_id=program.filter_id,
            )

            program2 = Program(group_name="FRITZ_TEST_AUTOSAVE", group_nickname="test2")
            Filter(
                collection="ZTF_alerts",
                group_id=program2.group_id,
                filter_id=program2.filter_id,
                autosave=True,
                pipeline=[{"$match": {"objectId": "ZTF20aaelulu"}}],
            )

            program3 = Program(
                group_name="FRITZ_TEST_UPDATE_ANNOTATIONS", group_nickname="test3"
            )
            Filter(
                collection="ZTF_alerts",
                group_id=program3.group_id,
                filter_id=program3.filter_id,
                update_annotations=True,
                pipeline=[
                    {"$match": {"objectId": "ZTF20aapcmur"}}
                ],  # there are 3 alerts in the test set for this oid
            )

        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f"ztf_{date}_programid1_test"
        path_alerts = "ztf_alerts/20200202"

        # grab some more alerts from gs://ztf-fritz/sample-public-alerts
        try:
            log("Grabbing more alerts from gs://ztf-fritz/sample-public-alerts")
            r = requests.get("https://www.googleapis.com/storage/v1/b/ztf-fritz/o")
            aa = r.json()["items"]
            ids = [pathlib.Path(a["id"]).parent for a in aa if "avro" in a["id"]]
        except Exception as e:
            log(
                "Grabbing alerts from gs://ztf-fritz/sample-public-alerts failed, but it is ok"
            )
            log(f"{e}")
            ids = []
        subprocess.run(
            [
                "gsutil",
                "-m",
                "cp",
                "-n",
                "gs://ztf-fritz/sample-public-alerts/*.avro",
                f"data/{path_alerts}",
            ]
        )
        log(f"Fetched {len(ids)} alerts from gs://ztf-fritz/sample-public-alerts")
        # push!
        with KafkaStream(
            topic_name,
            pathlib.Path(f"data/{path_alerts}"),
            config=config,
            test=True,
        ):
            log("Starting up Ingester")
            watchdog(obs_date=date, test=True)
            log("Digested and ingested: all done!")

        log("Checking the ZTF alert collection states")
        num_retries = 20
        # alert processing takes time, which depends on the available resources
        # so allow some additional time for the processing to finish
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError("Alert ingestion failed")

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            try:
                assert n_alerts == 313
                assert n_alerts_aux == 145
                break
            except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts}/{n_alerts_aux}, expecting 313/145). "
                    "Retrying in 15 seconds..."
                )
                time.sleep(15)
                continue

        if config["misc"]["broker"]:
            log("Checking that posting to SkyPortal succeeded")

            # check number of candidates that passed the first filter
            resp = requests.get(
                program.base_url + f"/api/candidates?groupIDs={program.group_id}",
                headers=program.headers,
                timeout=3,
            )

            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"
            assert "data" in result
            assert "totalMatches" in result["data"]
            assert result["data"]["totalMatches"] == 88

            # check that the only candidate that passed the second filter (ZTF20aaelulu) got saved as Source
            resp = requests.get(
                program2.base_url + f"/api/sources?group_ids={program2.group_id}",
                headers=program2.headers,
                timeout=3,
            )

            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"
            assert "data" in result
            assert "totalMatches" in result["data"]
            assert result["data"]["totalMatches"] == 1
            assert "sources" in result["data"]
            assert result["data"]["sources"][0]["id"] == "ZTF20aaelulu"


if __name__ == "__main__":
    test_ingester = TestIngester()
    test_ingester.test_ingester()
