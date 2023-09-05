import fastavro
import pytest
from kowalski.alert_brokers.alert_broker_ztf import ZTFAlertWorker
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


@pytest.fixture(autouse=True, scope="class")
def worker_fixture(request):
    log("Initializing alert processing worker class instance")
    request.cls.worker = ZTFAlertWorker()
    log("Successfully initialized")
    # do not attempt monitoring filters
    request.cls.run_forever = False


@pytest.fixture(autouse=True, scope="class")
def alert_fixture(request):
    log("Loading a sample ZTF alert")
    candid = 1127561445515015011
    request.cls.candid = candid
    sample_avro = f"data/ztf_alerts/20200202/{candid}.avro"
    with open(sample_avro, "rb") as f:
        for record in fastavro.reader(f):
            request.cls.alert = record
    log("Successfully loaded")


def filter_template(upstream):
    # prepend upstream aggregation stages:
    upstream_pipeline = config["database"]["filters"][upstream]
    pipeline = upstream_pipeline + [
        {
            "$match": {
                "candidate.drb": {"$gt": 0.5},
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
    # set permissions
    pipeline[0]["$match"]["candidate.programid"]["$in"] = [0, 1, 2, 3]
    pipeline[3]["$project"]["prv_candidates"]["$filter"]["cond"]["$and"][0]["$in"][
        1
    ] = [0, 1, 2, 3]

    template = {
        "group_id": 1,
        "filter_id": 1,
        "group_name": "test_group",
        "filter_name": "test_filter",
        "fid": "r4nd0m",
        "permissions": [0, 1, 2, 3],
        "autosave": False,
        "update_annotations": False,
        "pipeline": pipeline,
    }
    return template


def post_alert(worker, alert):
    alert, _ = worker.alert_mongify(alert)
    # check if it already exists
    if worker.mongo.db[worker.collection_alerts].count_documents(
        {"candid": alert["candid"]}
    ):
        log(f"Alert {alert['candid']} already exists, skipping")
        return
    worker.mongo.insert_one(collection=worker.collection_alerts, document=alert)


def delete_alert(worker, alert):
    worker.mongo.delete_one(
        collection=worker.collection_alerts,
        document={"candidate.candid": alert["candid"]},
    )


class TestAlertBrokerZTF:
    """Test individual components/methods of the ZTF alert processor used by the broker"""

    def test_alert_mongification(self):
        """Test massaging avro packet into a dict digestible by mongodb"""
        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        assert alert["candid"] == self.candid
        assert len(prv_candidates) == 31

    def test_make_photometry(self):
        df_photometry = self.worker.make_photometry(self.alert)
        assert len(df_photometry) == 32

    def test_make_thumbnails(self):
        alert, _ = self.worker.alert_mongify(self.alert)
        for ttype, istrument_type in [
            ("new", "Science"),
            ("ref", "Template"),
            ("sub", "Difference"),
        ]:
            thumb = self.worker.make_thumbnail(alert, ttype, istrument_type)
            assert len(thumb.get("data", [])) > 0

    def test_alert_filter__ml(self):
        """Test executing ML models on the alert"""
        alert, _ = self.worker.alert_mongify(self.alert)
        scores = self.worker.alert_filter__ml(alert)
        assert len(scores) > 0
        log(scores)

    def test_alert_filter__xmatch(self):
        """Test cross matching with external catalog"""
        alert, _ = self.worker.alert_mongify(self.alert)
        xmatches = self.worker.alert_filter__xmatch(alert)
        catalogs_to_xmatch = config["database"].get("xmatch", {}).get("ZTF", {}).keys()
        assert isinstance(xmatches, dict)
        assert len(list(xmatches.keys())) == len(catalogs_to_xmatch)
        assert set(xmatches.keys()) == set(catalogs_to_xmatch)
        assert all([isinstance(xmatches[c], list) for c in catalogs_to_xmatch])
        assert all([len(xmatches[c]) >= 0 for c in catalogs_to_xmatch])
        # TODO: add alerts with xmatches results to test against

    def test_alert_filter__user_defined(self):
        """Test pushing an alert through a filter"""
        post_alert(self.worker, self.alert)

        filter = filter_template(self.worker.collection_alerts)
        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)

        delete_alert(self.worker, self.alert)

        assert passed_filters is not None
        assert len(passed_filters) == 1

    def test_alert_filter__user_defined_followup(self):
        """Test pushing an alert through a filter that also has auto follow-up activated"""
        post_alert(self.worker, self.alert)
        filter = filter_template(self.worker.collection_alerts)
        filter["auto_followup"] = {
            "active": True,
            "pipeline": [
                {
                    "$match": {
                        "candidate.drb": {"$gt": 0.5},
                    }
                }
            ],
            "allocation_id": 1,
            "instrument_id": 1,
            "payload": {  # example payload for SEDM
                "observation_type": "IFU",
            },
        }
        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)
        delete_alert(self.worker, self.alert)

        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )

    def test_alert_filter__user_defined_followup_with_broker(self):
        """Test pushing an alert through a filter that also has auto follow-up activated, and broker mode activated"""
        if not config["misc"].get("broker", False):
            pytest.skip("Broker mode not activated, skipping")

        response = self.worker.api_skyportal("GET", "/api/allocation", None)
        assert response.status_code == 200
        allocations = response.json()["data"]
        # get the allocation which instrument['name'] == 'SEDM'
        allocations = [a for a in allocations if a["instrument"]["name"] == "SEDM"]
        assert len(allocations) == 1
        allocation_id = allocations[0]["id"]

        post_alert(self.worker, self.alert)

        filter = filter_template(self.worker.collection_alerts)
        filter["auto_followup"] = {
            "active": True,
            "pipeline": [
                {
                    "$match": {
                        "candidate.drb": {"$gt": 0.5},
                    }
                }
            ],
            "allocation_id": allocation_id,
            "payload": {  # example payload for SEDM
                "observation_type": "IFU",
            },
        }
        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)
        delete_alert(self.worker, self.alert)

        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )

        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

        # now fetch the follow-up request from SP
        response = self.worker.api_skyportal(
            "GET", f"/api/followup_request?sourceID={alert['objectId']}", None
        )
        assert response.status_code == 200
        followup_requests = response.json()["data"].get("followup_requests", [])
        followup_requests = [
            f
            for f in followup_requests
            if (f["allocation_id"] == allocation_id and f["status"] == "submitted")
        ]
        assert len(followup_requests) == 1
        assert followup_requests[0]["payload"]["observation_type"] == "IFU"
