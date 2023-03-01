import datetime
import os
import pathlib
import time

import requests
from alert_broker_pgir import watchdog
from ingester import KafkaStream
from test_ingester_ztf import Program
from utils import Mongo, init_db_sync, load_config, log

""" load config and secrets """
KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/app")
USING_DOCKER = os.environ.get("USING_DOCKER", False)
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]

if USING_DOCKER:
    config["server"]["host"] = "kowalski_api_1"


class Filter:
    def __init__(
        self,
        collection: str = "PGIR_alerts",
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
                        "annotations.mean_rb": {"$avg": "$prv_candidates.drb"},
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
        # path_kafka = pathlib.Path(config["path"]["kafka"])

        path_logs = pathlib.Path(config["path"]["logs"])
        if not path_logs.exists():
            path_logs.mkdir(parents=True, exist_ok=True)

        if config["misc"]["broker"]:
            log("Setting up test groups and filters in Fritz")
            program = Program(
                group_name="FRITZ_TEST_PGIR",
                group_nickname="test-pgir",
                filter_name="Infraorange transients",
                stream_ids=[4],
            )
            Filter(
                collection="PGIR_alerts",
                group_id=program.group_id,
                filter_id=program.filter_id,
                pipeline=[{"$match": {"candid": {"$gt": 0}}}],  # pass all
            )

            program2 = Program(
                group_name="FRITZ_TEST_PGIR_AUTOSAVE",
                group_nickname="test2-pgir",
                filter_name="Infraorange transients",
                stream_ids=[4],
            )
            Filter(
                collection="PGIR_alerts",
                group_id=program2.group_id,
                filter_id=program2.filter_id,
                autosave=True,
                pipeline=[{"$match": {"objectId": "PGIR19aacbvv"}}],
            )

            program3 = Program(
                group_name="FRITZ_TEST_PGIR_UPDATE_ANNOTATIONS",
                group_nickname="test3-pgir",
                filter_name="Infraorange transients",
                stream_ids=[4],
            )
            Filter(
                collection="PGIR_alerts",
                group_id=program3.group_id,
                filter_id=program3.filter_id,
                update_annotations=True,
                pipeline=[
                    {"$match": {"objectId": "PGIR21aeiljk"}}
                ],  # there are 3 alerts in the test set for this oid
            )

        # create a test PGIR topic for the current UTC date
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f"pgir_{date}_test"
        path_alerts = "pgir_alerts/20210629"

        with KafkaStream(
            topic_name,
            pathlib.Path(f"{KOWALSKI_APP_PATH}/data/{path_alerts}"),
            config=config,
            test=True,
        ):
            log("Starting up Ingester")
            watchdog(obs_date=date, test=True)
            log("Digested and ingested: all done!")

        log("Checking the PGIR alert collection states")
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
        collection_alerts = config["database"]["collections"]["alerts_pgir"]
        collection_alerts_aux = config["database"]["collections"]["alerts_pgir_aux"]

        num_retries = 10
        # alert processing takes time, which depends on the available resources
        # so allow some additional time for the processing to finish
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError("Alert ingestion failed")

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            # REMOVE THIS
            print("Testing n_alerts and n_alerts_aux", n_alerts, n_alerts_aux)
            try:
                assert n_alerts == 17
                assert n_alerts_aux == 15
                break
            except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts}/{n_alerts_aux}, expecting 17/15). "
                    "Retrying in 30 seconds..."
                )
                time.sleep(30)
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
            print("totalMatches", result["data"]["totalMatches"])
            # fixme:
            assert result["data"]["totalMatches"] == 15

            # check that the only candidate that passed the second filter (PGIR19aacbvv) got saved as Source
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
            assert result["data"]["sources"][0]["id"] == "PGIR19aacbvv"


if __name__ == "__main__":
    test_ingester = TestIngester()
    test_ingester.test_ingester()
