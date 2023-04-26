import datetime
import os
import pathlib
import time

import requests
from kowalski.alert_brokers.alert_broker_turbo import watchdog
from kowalski.ingesters.ingester import KafkaStream
from test_ingester_ztf import Program, Filter
from kowalski.utils import Mongo, init_db_sync
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

USING_DOCKER = os.environ.get("USING_DOCKER", False)
if USING_DOCKER:
    config["server"]["host"] = "kowalski_api_1"


class TestIngester:
    """ """

    def test_ingester(self):

        init_db_sync(config=config, verbose=True)

        log("Setting up paths")

        path_logs = pathlib.Path("logs/")
        if not path_logs.exists():
            path_logs.mkdir(parents=True, exist_ok=True)

        log("Checking the existing TURBO alert collection states")
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
        collection_alerts = config["database"]["collections"]["alerts_turbo"]
        collection_alerts_aux = config["database"]["collections"]["alerts_turbo_aux"]

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
            program = Program(
                group_name="FRITZ_TEST_TURBO",
                group_nickname="test-TURBO",
                filter_name="Infraorange transients",
                stream_ids=[4],
            )
            Filter(
                collection="TURBO_alerts",
                group_id=program.group_id,
                filter_id=program.filter_id,
                pipeline=[{"$match": {"candid": {"$gt": 0}}}],  # pass all
            )

            program2 = Program(
                group_name="FRITZ_TEST_TURBO_AUTOSAVE",
                group_nickname="test2-TURBO",
                filter_name="Infraorange transients",
                stream_ids=[4],
            )
            Filter(
                collection="TURBO_alerts",
                group_id=program2.group_id,
                filter_id=program2.filter_id,
                autosave=True,
                pipeline=[{"$match": {"objectId": "None.23"}}],
            )

            program3 = Program(
                group_name="FRITZ_TEST_TURBO_UPDATE_ANNOTATIONS",
                group_nickname="test3-TURBO",
                filter_name="Infraorange transients",
                stream_ids=[4],
            )
            Filter(
                collection="TURBO_alerts",
                group_id=program3.group_id,
                filter_id=program3.filter_id,
                update_annotations=True,
                pipeline=[
                    {"$match": {"objectId": "PGIR21aeiljk"}}
                ],  # there are 3 alerts in the test set for this oid
            )

        # create a test TURBO topic for the current UTC date
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f"turbo_{date}"
        path_alerts = pathlib.Path("turbo_alerts")
        # fixme: ONLY USING THE ARCHIVAL TURBO ALERTS FOR NOW

        with KafkaStream(
            topic_name,
            pathlib.Path(f"data/{path_alerts}"),
            config=config,
            test=True,
        ):
            log("Starting up Ingester")
            watchdog(obs_date=date, test=True)
            log("Digested and ingested: all done!")

        log("Checking the TURBO alert collection states")
        num_retries = 7
        # alert processing takes time, which depends on the available resources
        # so allow some additional time for the processing to finish
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError("Alert ingestion failed")

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            try:
                assert n_alerts == 11
                assert n_alerts_aux == 11
                print("----Passed TURBO ingester tests----")
                break
            except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts}/{n_alerts_aux}, expecting 11/11). "
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
            print("totalMatches", result["data"]["totalMatches"])
            # fixme:
            assert result["data"]["totalMatches"] == 11

            # check that the only candidate that passed the second filter (None.23) got saved as Source
            resp = requests.get(
                program2.base_url + f"/api/sources?group_ids={program2.group_id}",
                headers=program2.headers,
                timeout=3,
            )

            assert resp.status_code == requests.codes.ok
            result = resp.json()
            print(result["data"])
            assert result["status"] == "success"
            assert "data" in result
            assert "totalMatches" in result["data"]
            assert result["data"]["totalMatches"] == 1
            assert "sources" in result["data"]
            assert result["data"]["sources"][0]["id"] == "None.23"


if __name__ == "__main__":
    testIngest = TestIngester()
    testIngest.test_ingester()
