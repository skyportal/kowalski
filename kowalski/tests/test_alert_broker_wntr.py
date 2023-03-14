import fastavro
import pytest
from kowalski.alert_brokers.alert_broker_winter import WNTRAlertWorker
from kowalski.utils import log
from kowalski.config import load_config

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


@pytest.fixture(autouse=True, scope="class")
def worker_fixture(request):
    log("Initializing alert processing worker class instance")
    request.cls.worker = WNTRAlertWorker()
    log("Successfully initialized")
    # do not attempt monitoring filters
    request.cls.run_forever = False


@pytest.fixture(autouse=True, scope="class")
def alert_fixture(request):
    log("Loading a sample WNTR alert")
    # candid = 2459303860002
    candid = 2459362710041  # candidate with a prv_candidate field
    request.cls.candid = candid
    sample_avro = f"data/wntr_alerts/20220815/{candid}.avro"
    with open(sample_avro, "rb") as f:
        for record in fastavro.reader(f):
            request.cls.alert = record
    log("Successfully loaded")


class TestAlertBrokerWNTR:
    """Test individual components/methods of the WNTR alert processor used by the broker"""

    def test_alert_mongification(self):
        """Test massaging avro packet into a dict digestible by mongodb"""
        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        assert alert["candid"] == self.candid
        assert len(alert["candidate"]) > 0  # ensure cand data is not empty
        assert alert["objectId"] == self.alert["objectId"]
        assert len(prv_candidates) == 1  # 1 old candidate in prv_cand
        assert prv_candidates[0]["jd"] == self.alert["prv_candidates"][0]["jd"]

    def test_make_photometry(self):
        df_photometry = self.worker.make_photometry(self.alert)
        assert len(df_photometry) == 1
        assert df_photometry["isdiffpos"][0] == 1.0
        assert df_photometry["diffmaglim"][0] == 19.74010467529297
        assert df_photometry["filter"][0] == "2massj"

    def test_make_thumbnails(self):
        alert, _ = self.worker.alert_mongify(self.alert)
        for ttype, istrument_type in [
            ("new", "Science"),
            ("ref", "Template"),
            ("sub", "Difference"),
        ]:
            thumb = self.worker.make_thumbnail(alert, ttype, istrument_type)
            assert len(thumb.get("data", [])) > 0

    def test_alert_filter__xmatch(self):
        """Test cross matching with external catalog"""
        alert, _ = self.worker.alert_mongify(self.alert)
        xmatches = self.worker.alert_filter__xmatch(alert)
        catalogs_to_xmatch = config["database"].get("xmatch", {}).get("WNTR", {}).keys()
        assert isinstance(xmatches, dict)
        assert len(list(xmatches.keys())) == len(catalogs_to_xmatch)
        assert set(xmatches.keys()) == set(catalogs_to_xmatch)
        assert all([isinstance(xmatches[c], list) for c in catalogs_to_xmatch])
        assert all([len(xmatches[c]) >= 0 for c in catalogs_to_xmatch])
        # TODO: add alerts with xmatches results to test against

    def test_alert_filter__xmatch_clu(self):
        """Test cross matching with the CLU catalog"""
        alert, _ = self.worker.alert_mongify(self.alert)
        xmatches_clu = self.worker.alert_filter__xmatch_clu(
            alert, clu_version="CLU_TEST"
        )
        assert isinstance(xmatches_clu, dict)
        assert len(xmatches_clu.keys()) == 1
        assert "CLU_TEST" in xmatches_clu.keys()
        assert isinstance(xmatches_clu["CLU_TEST"], list)
        assert len(xmatches_clu["CLU_TEST"]) >= 0
        # TODO: add alerts with xmatches results to test against

    def test_alert_filter__user_defined(self):
        """Test pushing an alert through a filter"""
        # prepend upstream aggregation stages:
        upstream_pipeline = config["database"]["filters"][self.worker.collection_alerts]
        pipeline = upstream_pipeline + [
            {
                "$addFields": {
                    "annotations.author": "dd",
                }
            },
            {"$project": {"_id": 0, "candid": 1, "objectId": 1, "annotations": 1}},
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
            [filter_template], self.alert
        )
        assert passed_filters is not None
