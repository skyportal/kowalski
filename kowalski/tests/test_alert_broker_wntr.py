import fastavro
import pytest
from copy import deepcopy
from kowalski.alert_brokers.alert_broker_winter import WNTRAlertWorker
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


@pytest.fixture(autouse=True, scope="class")
def worker_fixture(request):
    log("Initializing alert processing worker class instance")
    request.cls.worker = WNTRAlertWorker()
    log("Successfully initialized")
    # do not attempt monitoring filters
    request.cls.worker.run_forever = False


@pytest.fixture(autouse=True, scope="class")
def alert_fixture(request):
    log("Loading a sample WNTR alert")
    candid = 3608694  # candidate with a prv_candidate field
    request.cls.candid = candid
    sample_avro = f"data/wntr_alerts/20240311/{candid}.avro"
    with open(sample_avro, "rb") as f:
        for record in fastavro.reader(f):
            request.cls.alert = record
    log("Successfully loaded")


def post_alert(worker: WNTRAlertWorker, alert):
    delete_alert(worker, alert)
    alert, prv_candidates, fp_hists = worker.alert_mongify(alert)
    # check if it already exists
    if worker.mongo.db[worker.collection_alerts].count_documents(
        {"candid": alert["candid"]}
    ):
        log(f"Alert {alert['candid']} already exists, skipping")
    else:
        worker.mongo.insert_one(collection=worker.collection_alerts, document=alert)

    if worker.mongo.db[worker.collection_alerts_aux].count_documents(
        {"_id": alert["objectid"]}
    ):
        # delete if it exists
        worker.mongo.delete_one(
            collection=worker.collection_alerts_aux,
            document={"_id": alert["objectid"]},
        )

    aux = {
        "_id": alert["objectid"],
        "prv_candidates": prv_candidates,
        "cross_matches": {},
    }
    worker.mongo.insert_one(collection=worker.collection_alerts_aux, document=aux)


def delete_alert(worker, alert):
    worker.mongo.delete_one(
        collection=worker.collection_alerts,
        document={"candidate.candid": alert["candid"]},
    )
    worker.mongo.delete_one(
        collection=worker.collection_alerts_aux,
        document={"_id": alert["objectid"]},
    )


class TestAlertBrokerWNTR:
    """Test individual components/methods of the WNTR alert processor used by the broker"""

    def test_alert_mongification(self):
        """Test massaging avro packet into a dict digestible by mongodb"""
        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        assert alert["candid"] == self.candid
        assert len(alert["candidate"]) > 0  # ensure cand data is not empty
        assert alert["objectid"] == self.alert["objectid"]
        assert len(prv_candidates) == 1  # 1 old candidate in prv_cand
        assert prv_candidates[0]["jd"] == self.alert["prv_candidates"][0]["jd"]

    def test_make_photometry(self):
        df_photometry = self.worker.make_photometry(self.alert)
        assert len(df_photometry) == 1
        assert df_photometry["isdiffpos"][0] == 1.0
        assert df_photometry["diffmaglim"][0] == 17.6939640045166
        assert df_photometry["filter"][0] == "2massj"

    def test_make_thumbnails(self):
        alert, _, _ = self.worker.alert_mongify(self.alert)
        # just like in the alert broker, to avoid having if statement just to grab the
        # winter alert packet fields that are not the same as other instruments, we first add aliases for those
        assert "cutout_science" in alert
        assert "cutout_template" in alert
        assert "cutout_difference" in alert
        assert "objectid" in alert
        alert["cutoutScience"] = alert.get("cutout_science")
        alert["cutoutTemplate"] = alert.get("cutout_template")
        alert["cutoutDifference"] = alert.get("cutout_difference")
        alert["objectId"] = alert.get("objectid")
        for ttype, istrument_type in [
            ("new", "Science"),
            ("ref", "Template"),
            ("sub", "Difference"),
        ]:
            thumb = self.worker.make_thumbnail(alert, ttype, istrument_type)
            assert len(thumb.get("data", [])) > 0

    def test_alert_filter__xmatch(self):
        """Test cross matching with external catalog"""
        alert, _, _ = self.worker.alert_mongify(self.alert)
        xmatches = self.worker.alert_filter__xmatch(alert)
        catalogs_to_xmatch = config["database"].get("xmatch", {}).get("WNTR", {}).keys()
        assert isinstance(xmatches, dict)
        assert len(list(xmatches.keys())) == len(catalogs_to_xmatch)
        assert set(xmatches.keys()) == set(catalogs_to_xmatch)
        assert all([isinstance(xmatches[c], list) for c in catalogs_to_xmatch])
        assert all([len(xmatches[c]) >= 0 for c in catalogs_to_xmatch])
        # TODO: add alerts with xmatches results to test against

    def test_alert_filter__user_defined(self):
        """Test pushing an alert through a filter"""
        post_alert(self.worker, self.alert)
        alert = deepcopy(self.alert)

        alert["objectId"] = alert["objectid"]
        upstream_pipeline = config["database"]["filters"][self.worker.collection_alerts]
        pipeline = upstream_pipeline + [
            {
                "$addFields": {
                    "annotations.author": "dd",
                }
            },
            {"$project": {"_id": 0, "candid": 1, "objectid": 1, "annotations": 1}},
        ]

        filter_template = {
            "group_id": 1,
            "filter_id": 1,
            "group_name": "test_group",
            "filter_name": "test_filter",
            "fid": "r4nd0m",
            "permissions": [0, 1],
            "autosave": False,
            "update_annotations": False,
            "pipeline": pipeline,
        }
        passed_filters = self.worker.alert_filter__user_defined(
            [filter_template], alert
        )
        delete_alert(self.worker, self.alert)

        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert passed_filters[0]["data"]["candid"] == self.alert["candid"]
        assert passed_filters[0]["data"]["objectid"] == self.alert["objectid"]
        assert passed_filters[0]["data"]["annotations"]["author"] == "dd"
