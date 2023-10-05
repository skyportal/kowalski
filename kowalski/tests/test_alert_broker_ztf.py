import fastavro
import pytest
from kowalski.alert_brokers.alert_broker_ztf import ZTFAlertWorker
from kowalski.config import load_config
from kowalski.log import log
from copy import deepcopy

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


@pytest.fixture(autouse=True, scope="class")
def worker_fixture(request):
    log("Initializing alert processing worker class instance")
    request.cls.worker = ZTFAlertWorker()
    log("Successfully initialized")
    # do not attempt monitoring filters
    request.cls.worker.run_forever = False


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
        "group_id": 2,
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
        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        all_prv_candidates = deepcopy(prv_candidates) + [deepcopy(alert["candidate"])]
        scores = self.worker.alert_filter__ml(alert, all_prv_candidates)
        assert len(scores) > 0
        # print(scores)

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
        assert "autosave" in passed_filters[0]

    def test_alert_filter__user_defined_autosave(self):
        """Test pushing an alert through a filter with and without autosave"""
        post_alert(self.worker, self.alert)

        # try with the autosave set to False (default)
        filter = filter_template(self.worker.collection_alerts)
        passed_filters_no_autosave = self.worker.alert_filter__user_defined(
            [filter], self.alert
        )

        # try with the autosave set to True
        filter["autosave"] = True
        passed_filters_autosave = self.worker.alert_filter__user_defined(
            [filter], self.alert
        )

        # try with a complex autosave key (with comment)
        filter["autosave"] = {
            "active": True,
            "comment": "Saved to BTS by BTSbot.",
        }
        passed_filters_complex_autosave = self.worker.alert_filter__user_defined(
            [filter], self.alert
        )

        # try without specifying an autosave key
        filter.pop("autosave")
        passed_filters_no_autosave_key = self.worker.alert_filter__user_defined(
            [filter], self.alert
        )

        delete_alert(self.worker, self.alert)

        assert passed_filters_no_autosave is not None
        assert len(passed_filters_no_autosave) == 1
        assert "autosave" in passed_filters_no_autosave[0]
        assert passed_filters_no_autosave[0]["autosave"] is False

        assert passed_filters_autosave is not None
        assert len(passed_filters_autosave) == 1
        assert "autosave" in passed_filters_autosave[0]
        assert passed_filters_autosave[0]["autosave"] is True

        assert passed_filters_no_autosave_key is not None
        assert len(passed_filters_no_autosave_key) == 1
        assert "autosave" in passed_filters_no_autosave_key[0]
        assert passed_filters_no_autosave_key[0]["autosave"] is False

        assert passed_filters_complex_autosave is not None
        assert len(passed_filters_complex_autosave) == 1
        assert "autosave" in passed_filters_complex_autosave[0]
        assert "comment" in passed_filters_complex_autosave[0]["autosave"]
        assert (
            passed_filters_complex_autosave[0]["autosave"]["comment"]
            == "Saved to BTS by BTSbot."
        )

    def test_alert_filter__user_defined_followup(self):
        """Test pushing an alert through a filter that also has auto follow-up activated"""
        post_alert(self.worker, self.alert)
        filter = filter_template(self.worker.collection_alerts)
        filter["autosave"] = True
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
                "priority": 3,
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
        assert passed_filters[0]["auto_followup"]["data"]["payload"]["priority"] == 3

    def test_alert_filter__user_defined_followup_with_broker(self):
        """Test pushing an alert through a filter that also has auto follow-up activated, and broker mode activated"""
        if not config["misc"].get("broker", False):
            pytest.skip("Broker mode not activated, skipping")

        # first get the groups
        response = self.worker.api_skyportal("GET", "/api/groups", None)
        # find the group called 'Program A'
        groups = response.json()["data"]["user_accessible_groups"]
        saved_group_id = [g for g in groups if g["name"] == "Program A"][0]["id"]
        ignore_if_saved_group_id = [g for g in groups if g["name"] == "Program B"][0][
            "id"
        ]
        target_group_id = [g for g in groups if g["name"] == "Sitewide Group"][0]["id"]

        assert saved_group_id is not None
        assert ignore_if_saved_group_id is not None
        assert target_group_id is not None

        response = self.worker.api_skyportal("GET", "/api/allocation", None)
        assert response.status_code == 200
        allocations = response.json()["data"]
        # get the allocation which instrument['name'] == 'SEDM'
        allocations = [a for a in allocations if a["instrument"]["name"] == "SEDM"]
        assert len(allocations) == 1
        allocation_id = allocations[0]["id"]

        # first fetch the existing follow-up request from SP
        response = self.worker.api_skyportal(
            "GET", "/api/followup_request?sourceID=ZTF20aajcbhr", None
        )
        assert response.status_code == 200
        followup_requests = response.json()["data"].get("followup_requests", [])
        # get their ids
        followup_request_ids = [f["id"] for f in followup_requests]
        # delete them
        for followup_request_id in followup_request_ids:
            response = self.worker.api_skyportal(
                "DELETE", f"/api/followup_request/{followup_request_id}", None
            )
            assert response.status_code == 200

        post_alert(self.worker, self.alert)

        filter = filter_template(self.worker.collection_alerts)
        filter["group_id"] = saved_group_id
        filter["autosave"] = {
            "active": True,
            "comment": "Saved to BTS by BTSbot.",
            "pipeline": [
                {
                    "$match": {
                        "candidate.drb": {"$gt": 0.5},
                    }
                }
            ],
        }
        filter["auto_followup"] = {
            "active": True,
            "pipeline": [
                {
                    "$match": {
                        "candidate.drb": {"$gt": 0.5},
                    }
                }
            ],
            "comment": "SEDM triggered by BTSbot",
            "allocation_id": allocation_id,
            "payload": {  # example payload for SEDM
                "observation_type": "IFU",
                "priority": 2,
                "advanced": False,
            },
        }
        # make a copy of that filter, but with priority 3
        filter2 = deepcopy(filter)
        filter2["auto_followup"]["payload"]["priority"] = 3
        passed_filters = self.worker.alert_filter__user_defined(
            [filter, filter2], self.alert
        )

        assert passed_filters is not None
        assert len(passed_filters) == 2  # both filters should have passed
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )
        assert passed_filters[0]["auto_followup"]["data"]["payload"]["priority"] == 2
        assert "auto_followup" in passed_filters[1]
        assert (
            passed_filters[1]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )
        assert passed_filters[1]["auto_followup"]["data"]["payload"]["priority"] == 3

        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

        # now fetch the follow-up request from SP
        # it should have deduplicated and used the highest priority
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
        assert (
            followup_requests[0]["payload"]["priority"] == 3
        )  # it should have deduplicated and used the highest priority

        # now run it once more, but with a higher priority to see if the update works
        filter2["auto_followup"]["payload"]["priority"] = 4
        passed_filters = self.worker.alert_filter__user_defined(
            [filter, filter2], self.alert
        )

        assert passed_filters is not None
        assert len(passed_filters) == 2
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )
        assert passed_filters[0]["auto_followup"]["data"]["payload"]["priority"] == 2
        assert "auto_followup" in passed_filters[1]
        assert (
            passed_filters[1]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )
        assert passed_filters[1]["auto_followup"]["data"]["payload"]["priority"] == 4

        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

        # now fetch the follow-up request from SP
        # it should have deduplicated and used the highest priority
        response = self.worker.api_skyportal(
            "GET", f"/api/followup_request?sourceID={alert['objectId']}", None
        )
        assert response.status_code == 200
        followup_requests_updated = response.json()["data"].get("followup_requests", [])
        followup_requests_updated = [
            f
            for f in followup_requests_updated
            if (f["allocation_id"] == allocation_id and f["status"] == "submitted")
        ]
        assert len(followup_requests_updated) == 1
        assert followup_requests_updated[0]["payload"]["observation_type"] == "IFU"
        assert (
            followup_requests_updated[0]["payload"]["priority"] == 4
        )  # it should have deduplicated and used the highest priority
        assert (
            followup_requests_updated[0]["id"] == followup_requests[0]["id"]
        )  # the id should be the same

        # now, we'll test the target_group_ids functionality for deduplication
        # that is, if a filter has a target_group_ids, then it should only trigger a follow-up request
        # if none of the existing requests' target groups have an overlap with the target_group_ids
        # of the filter

        filter_multiple_groups = deepcopy(filter)
        filter_multiple_groups["auto_followup"]["target_group_ids"] = [saved_group_id]
        filter_multiple_groups["group_id"] = target_group_id

        passed_filters = self.worker.alert_filter__user_defined(
            [filter_multiple_groups], self.alert
        )

        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )
        assert passed_filters[0]["auto_followup"]["data"]["payload"]["priority"] == 2
        assert set(
            passed_filters[0]["auto_followup"]["data"]["target_group_ids"]
        ).issubset(set([target_group_id] + [saved_group_id]))

        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

        # now fetch the follow-up request from SP
        # we should still have just one follow-up request, the exact same as before
        response = self.worker.api_skyportal(
            "GET", f"/api/followup_request?sourceID={alert['objectId']}", None
        )
        assert response.status_code == 200
        followup_requests_updated = response.json()["data"].get("followup_requests", [])
        followup_requests_updated = [
            f
            for f in followup_requests_updated
            if (f["allocation_id"] == allocation_id and f["status"] == "submitted")
        ]
        assert len(followup_requests_updated) == 1
        assert followup_requests_updated[0]["payload"]["observation_type"] == "IFU"
        assert followup_requests_updated[0]["payload"]["priority"] == 4
        assert followup_requests_updated[0]["id"] == followup_requests[0]["id"]

        # delete the follow-up request
        response = self.worker.api_skyportal(
            "DELETE", f"/api/followup_request/{followup_requests[0]['id']}", None
        )
        assert response.status_code == 200

        # now we'll try to push the same alert through the same filter, but with a different group_id
        # and using the ignore_if_saved_group_id, to verify that it doesn't get saved, and doesnt trigger a follow-up request
        filter["group_id"] = ignore_if_saved_group_id
        filter["autosave"] = {
            **filter["autosave"],
            "ignore_group_ids": [saved_group_id],
            "comment": "Saved to BTS2 by BTSbot.",
        }
        filter["auto_followup"] = {
            **filter["auto_followup"],
            "comment": "SEDM triggered by BTSbot2",
        }

        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)
        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )

        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

        # now fetch the source from SP
        response = self.worker.api_skyportal(
            "GET", f"/api/sources/{alert['objectId']}", None
        )
        assert response.status_code == 200
        source = response.json()["data"]
        assert source["id"] == "ZTF20aajcbhr"
        # should be saved to Program A and Sitewide Group, but not Program B
        assert any([g["id"] == saved_group_id for g in source["groups"]])
        assert any([g["id"] == target_group_id for g in source["groups"]])
        assert not any([g["id"] == ignore_if_saved_group_id for g in source["groups"]])

        # verify that there isn't a follow-up request
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
        assert len(followup_requests) == 0

        # rerun the first filter, but not with the ignore_if_saved_group_id
        # this time, we are testing that it does not trigger a follow-up request
        # if the source is already classified

        # first post a classification
        response = self.worker.api_skyportal(
            "POST",
            "/api/classification",
            {
                "obj_id": alert["objectId"],
                "classification": "Ia",
                "probability": 0.8,
                "taxonomy_id": 1,
                "group_ids": [saved_group_id],
            },
        )
        assert response.status_code == 200
        classification_id = response.json()["data"]["classification_id"]
        assert classification_id is not None

        # now rerun the filter
        filter["group_id"] = saved_group_id
        del filter["autosave"]["ignore_group_ids"]

        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)
        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )

        alert, prv_candidates = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

        # now fetch the source from SP
        response = self.worker.api_skyportal(
            "GET", f"/api/sources/{alert['objectId']}", None
        )
        assert response.status_code == 200
        source = response.json()["data"]
        assert source["id"] == "ZTF20aajcbhr"

        # should only be saved to Program A and Sitewide Group
        assert any([g["id"] == saved_group_id for g in source["groups"]])
        assert any([g["id"] == target_group_id for g in source["groups"]])

        # verify that there is a follow-up request
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
        assert len(followup_requests) == 0

        # delete the classification
        response = self.worker.api_skyportal(
            "DELETE", f"/api/classification/{classification_id}", None
        )

        # unsave the source from the group
        response = self.worker.api_skyportal(
            "POST",
            "/api/source_groups",
            {
                "objId": alert["objectId"],
                "unsaveGroupIds": [saved_group_id],
            },
        )
        assert response.status_code == 200

        delete_alert(self.worker, self.alert)
