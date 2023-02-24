import datetime
import os
import pathlib
import time

import requests
from alert_broker_winter import watchdog
from ingester import KafkaStream
from test_ingester import Program
from utils import Mongo, init_db_sync, load_config, log

"""
Essentially the same as the ZTF ingester tests (test_ingester.py), except -
1. Only a small number of test WINTER/WIRC alerts are used. These alerts are stored in data/, as opposed to ZTF test_ingester.py that pulls alerts from googleapis.
2. Collections and Skyportal programs and filters relevant to WINTER are used instead of the ztf ones.
"""

""" load config and secrets """
KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/app")
USING_DOCKER = os.environ.get("USING_DOCKER", False)
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]

if USING_DOCKER:
    config["server"]["host"] = "kowalski_api_1"


class Filter:
    def __init__(
        self,
        collection: str = "WNTR_alerts",
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
        print(f"token: {token}")

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
    """ """

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
                group_name="FRITZ_TEST_WNTR",
                group_nickname="test-wntr",
                filter_name="Infraorange transients",
                stream_ids=[5],
            )
            Filter(
                collection="WNTR_alerts",
                group_id=program.group_id,
                filter_id=program.filter_id,
                pipeline=[{"$match": {"candid": {"$gt": 0}}}],  # pass all
            )

            program2 = Program(
                group_name="FRITZ_TEST_WNTR_AUTOSAVE",
                group_nickname="test2-wntr",
                filter_name="Infraorange transients",
                stream_ids=[5],
            )
            Filter(
                collection="WNTR_alerts",
                group_id=program2.group_id,
                filter_id=program2.filter_id,
                autosave=True,
                pipeline=[{"$match": {"objectId": "WIRC21aaaab"}}],
            )

            program3 = Program(
                group_name="FRITZ_TEST_WNTR_UPDATE_ANNOTATIONS",
                group_nickname="test3-wntr",
                filter_name="Infraorange transients",
                stream_ids=[5],
            )
            Filter(
                collection="WNTR_alerts",
                group_id=program3.group_id,
                filter_id=program3.filter_id,
                update_annotations=True,
                pipeline=[
                    {"$match": {"objectId": "WIRC21aaaac"}}
                ],  # there are 2 alerts in the test set for this oid
            )

        # create a test WNTR topic for the current UTC date
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f"winter_{date}_test"
        path_alerts = "wntr_alerts/20220815"

        with KafkaStream(
            topic_name,
            pathlib.Path(f"{KOWALSKI_APP_PATH}/data/{path_alerts}"),
            config=config,
            test=True,
        ):
            log("Starting up Ingester")
            watchdog(obs_date=date, test=True)
            log("Digested and ingested: all done!")

        log("Checking the WNTR alert collection states")
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
        collection_alerts = config["database"]["collections"]["alerts_wntr"]
        collection_alerts_aux = config["database"]["collections"]["alerts_wntr_aux"]

        num_retries = 10
        # alert processing takes time, which depends on the available resources
        # so allow some additional time for the processing to finish
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError("WNTR Alert ingestion failed")

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            try:
                assert n_alerts == 5
                assert n_alerts_aux == 4
                print("----Passed WNTR ingester tests----")
                break
            except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts}/{n_alerts_aux}, expecting 5/4). "
                    "Retrying in 30 seconds..."
                )
                time.sleep(30)
                continue


if __name__ == "__main__":
    testIngest = TestIngester()
    testIngest.test_ingester()
