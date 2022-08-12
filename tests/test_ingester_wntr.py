from confluent_kafka import Producer
import datetime
import os
import pathlib
import requests
import subprocess
import time

from alert_broker_pgir import watchdog
from test_ingester import Program
from utils import init_db_sync, load_config, log, Mongo


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]

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
        self.group_id = int(group_id)
        self.filter_id = int(filter_id)
        self.autosave = autosave
        self.update_annotations = update_annotations
        self.permissions = permissions if permissions is not None else [1, 2]

        self.pipeline = pipeline
        # TODO pipieline mongoDB cmd needed?


        self.fid = self.create()

    @staticmethod
    def get_api_token():
        a = requests.post(
            f"http://kowalski_api_1:{config['server']['port']}/api/auth",
            json={
                "username": config["server"]["admin_username"],
                "password": config["server"]["admin_password"],
            },
        )
        credentials = a.json()
        token = credentials.get("token", None)
        print(f'token: {token}')

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
            f"http://kowalski_api_1:{config['server']['port']}/api/filters",
            json=user_filter,
            headers=self.headers,
            timeout=5,
        )
        assert resp.status_code == requests.codes.ok
        result = resp.json()
        print(f'result: {result}')
        assert result["status"] == "success"
        assert "data" in result
        assert "fid" in result["data"]
        fid = result["data"]["fid"]
        print(f'fid: {fid}')
        return fid     

    def remove(self):
        resp = requests.delete(
            f"http://kowalski_api_1:{config['server']['port']}/api/filters",
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

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        log(f"Message delivery failed: {err}")
    else:
        log(f"Message delivered to {msg.topic()} [{msg.partition()}]")

class TestIngester:
    """
    
    """
    def read_mongoDB(self):
        """Query mongoDB for number of rows/alerts in table."""
        print("Reading MongoDB collection states")
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=True,
        )
        collection_alerts_wntr = config["database"]["collections"]["alerts_wntr"]
        collection_alerts_aux_wntr = config["database"]["collections"]["alerts_wntr_aux"]

        n_alerts_wntr = mongo.db[collection_alerts_wntr].count_documents({})
        n_alerts_aux_wntr = mongo.db[collection_alerts_aux_wntr].count_documents({})

        collection_alerts_pgir = config["database"]["collections"]["alerts_pgir"]
        collection_alerts_aux_pgir = config["database"]["collections"]["alerts_pgir_aux"]

        n_alerts_pgir = mongo.db[collection_alerts_pgir].count_documents({})
        n_alerts_aux_pgir = mongo.db[collection_alerts_aux_pgir].count_documents({})

        # REMOVE THIS
        print(f"WINTER: {n_alerts_wntr} alerts, {n_alerts_aux_wntr} alerts_aux")
        print(f"PGIR: {n_alerts_pgir} alerts, {n_alerts_aux_pgir} alerts_aux")

        # TODO check
        try: 
            assert n_alerts_wntr == 4
            assert n_alerts_aux_wntr == 0
        except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts_wntr}/{n_alerts_aux_wntr}, expecting 4/0). "
                )


if __name__ == "__main__":
    testIngest = TestIngester()
    testIngest.read_mongoDB()

    # testFilter = Filter()
    # print(f'finished tests in __main__')