from copy import deepcopy
from datetime import datetime

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


def filter_template(upstream, include_fp_hists=False):
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
        {
            "$project": {
                "_id": 0,
                "candid": 1,
                "objectId": 1,
                "annotations": 1,
                "t_now": "$candidate.jd",
            }
        },
    ]
    # if include_fp_hists:
    if include_fp_hists:
        pipeline[-1]["$project"]["fp_hists"] = 1

    # set permissions
    pipeline[0]["$match"]["candidate.programid"]["$in"] = [0, 1, 2, 3]
    pipeline[3]["$project"]["prv_candidates"]["$filter"]["cond"]["$and"][0]["$in"][
        1
    ] = [0, 1, 2, 3]

    pipeline[3]["$project"]["fp_hists"]["$filter"]["cond"]["$and"][0]["$in"][1] = [
        0,
        1,
        2,
        3,
    ]

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


def post_alert(worker: ZTFAlertWorker, alert, fp_cutoff=1):
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
        {"_id": alert["objectId"]}
    ):
        # delete if it exists
        worker.mongo.delete_one(
            collection=worker.collection_alerts_aux,
            document={"_id": alert["objectId"]},
        )

    # fp_hists: pop nulls - save space
    fp_hists = [
        {kk: vv for kk, vv in fp_hist.items() if vv not in [None, -99999, -99999.0]}
        for fp_hist in fp_hists
    ]

    fp_hists = worker.format_fp_hists(alert, fp_hists)

    # sort fp_hists by jd
    fp_hists = sorted(fp_hists, key=lambda x: x["jd"])

    if fp_cutoff < 1:
        index = int(fp_cutoff * len(fp_hists))
        fp_hists = fp_hists[:index]

    aux = {
        "_id": alert["objectId"],
        "prv_candidates": prv_candidates,
        "cross_matches": {},
        "fp_hists": fp_hists,
    }
    worker.mongo.insert_one(collection=worker.collection_alerts_aux, document=aux)


def delete_alert(worker, alert):
    worker.mongo.delete_one(
        collection=worker.collection_alerts,
        document={"candidate.candid": alert["candid"]},
    )
    worker.mongo.delete_one(
        collection=worker.collection_alerts_aux,
        document={"_id": alert["objectId"]},
    )


class TestAlertBrokerZTF:
    """Test individual components/methods of the ZTF alert processor used by the broker"""

    def test_alert_mongification(self):
        """Test massaging avro packet into a dict digestible by mongodb"""
        alert, prv_candidates, fp_hists = self.worker.alert_mongify(self.alert)
        assert alert["candid"] == self.candid
        assert len(prv_candidates) == 31
        assert len(fp_hists) == 0

    def test_alert_mongofication_with_fphists(self):
        """Test massaging avro packet into a dict digestible by mongodb, with the new ZTF alert schema"""
        candid = 2475433850015010009
        sample_avro = f"data/ztf_alerts/20231012/{candid}.avro"
        with open(sample_avro, "rb") as f:
            for record in fastavro.reader(f):
                alert, prv_candidates, fp_hists = self.worker.alert_mongify(record)
                assert alert["candid"] == candid
                assert len(prv_candidates) == 20
                assert len(fp_hists) == 21
                # verify that they are in order (jd, oldest to newest)
                assert all(
                    [
                        fp_hists[i]["jd"] < fp_hists[i + 1]["jd"]
                        for i in range(len(fp_hists) - 1)
                    ]
                )

                # test the removal of nulls and bad fields (None, -99999, -99999.0)
                assert "forcediffimflux" in fp_hists[0]
                fp_hists = [
                    {
                        kk: vv
                        for kk, vv in fp_hist.items()
                        if vv not in [None, -99999, -99999.0]
                    }
                    for fp_hist in fp_hists
                ]
                assert "forcediffimflux" not in fp_hists[0]

                # FOR NOW: we decided to only store the forced photometry for the very first alert we get for an object
                # so, no need to update anything here

                # # test the merging of existing fp_hists and new fp_hists
                # # we split the existing fp_hists in two to simulate the case where
                # # we have overlapping datapoints
                # existing_fp_hists = fp_hists[:15]
                # latest_fp_hists = fp_hists[11:]

                # new_fp_hists = self.worker.deduplicate_fp_hists(
                #     existing_fp_hists, latest_fp_hists
                # )
                # assert len(new_fp_hists) == 21

                # # verify that their jds are in order
                # assert all(
                #     [
                #         new_fp_hists[i]["jd"] < new_fp_hists[i + 1]["jd"]
                #         for i in range(len(new_fp_hists) - 1)
                #     ]
                # )
                # # verify that the datapoints are the exact same as original fp_hists
                # # that is, that all the keys are the same, and that the values are the same
                # for i in range(len(new_fp_hists)):
                #     assert (
                #         set(new_fp_hists[i].keys()).difference(set(fp_hists[i].keys()))
                #         == set()
                #     )
                #     for k in new_fp_hists[i].keys():
                #         assert new_fp_hists[i][k] == fp_hists[i][k]

    def test_ingest_alert_with_fp_hists(self):
        candid = 2475433850015010009
        sample_avro = f"data/ztf_alerts/20231012/{candid}.avro"
        alert_mag = 21.0
        fp_hists = []
        with open(sample_avro, "rb") as f:
            records = [record for record in fastavro.reader(f)]
            for record in records:
                # delete_alert(self.worker, record)
                alert, prv_candidates, fp_hists = self.worker.alert_mongify(record)
                alert_mag = alert["candidate"]["magpsf"]
                post_alert(self.worker, record, fp_cutoff=0.7)

        # verify that the alert was ingested
        assert (
            self.worker.mongo.db[self.worker.collection_alerts].count_documents(
                {"candid": candid}
            )
            == 1
        )
        assert (
            self.worker.mongo.db[self.worker.collection_alerts_aux].count_documents(
                {"_id": record["objectId"]}
            )
            == 1
        )

        # verify that fp_hists was ingested correctly
        aux = self.worker.mongo.db[self.worker.collection_alerts_aux].find_one(
            {"_id": record["objectId"]}
        )
        assert "fp_hists" in aux
        assert len(aux["fp_hists"]) == 14  # we had a cutoff at 21 * 0.7 = 14.7, so 14

        # print("---------- Original ----------")
        # for fp in aux["fp_hists"]:
        #     print(f"{fp['jd']}: {fp['alert_mag']}")

        # fp_hists: pop nulls - save space and make sure its the same as what is in the DB
        fp_hists = [
            {kk: vv for kk, vv in fp_hist.items() if vv not in [None, -99999, -99999.0]}
            for fp_hist in fp_hists
        ]

        # sort fp_hists by jd
        fp_hists = sorted(fp_hists, key=lambda x: x["jd"])

        # now, let's try the alert worker's update_fp_hists method, passing it the full fp_hists
        # and verify that it we have 21 exactly

        # first we add some forced photometry, where we have new rows, but overlapping rows from a fainter alert
        record["candidate"]["magpsf"] = 30.0
        # keep the last 10 fp_hists
        fp_hists_copy = fp_hists[-10:]
        # remove the last 2
        fp_hists_copy = fp_hists_copy[:-2]

        fp_hists_formatted = self.worker.format_fp_hists(alert, fp_hists_copy)
        self.worker.update_fp_hists(record, fp_hists_formatted)

        aux = self.worker.mongo.db[self.worker.collection_alerts_aux].find_one(
            {"_id": record["objectId"]}
        )
        # print(aux)
        assert "fp_hists" in aux
        assert len(aux["fp_hists"]) == 19

        # print("---------- First update ----------")
        # for fp in aux["fp_hists"]:
        #     print(f"{fp['jd']}: {fp['alert_mag']}")

        # we should have the same first 14 fp_hists as before, then the new ones
        assert all([aux["fp_hists"][i]["alert_mag"] == alert_mag for i in range(14)])
        assert all([aux["fp_hists"][i]["alert_mag"] == 30.0 for i in range(14, 19)])

        # verify they are still in order by jd (oldest to newest)
        assert all(
            [
                aux["fp_hists"][i]["jd"] < aux["fp_hists"][i + 1]["jd"]
                for i in range(len(aux["fp_hists"]) - 1)
            ]
        )

        # now, the last 10 datapoints, but as if they were from a brighter alert
        # we should have an overlap with both the original FP, and the datapoints (from faint alert) we just added
        record["candidate"]["magpsf"] = 15.0
        # keep the last 10 fp_hists
        fp_hists_copy = fp_hists[-10:]

        fp_hists_formatted = self.worker.format_fp_hists(alert, fp_hists_copy)
        self.worker.update_fp_hists(record, fp_hists_formatted)

        aux = self.worker.mongo.db[self.worker.collection_alerts_aux].find_one(
            {"_id": record["objectId"]}
        )

        assert "fp_hists" in aux
        assert len(aux["fp_hists"]) == 21

        # print("---------- Last update ----------")
        # for fp in aux["fp_hists"]:
        #     print(f"{fp['jd']}: {fp['alert_mag']}")

        # the result should be 21 fp_hists, with the first 11 being the same as before, and the last 10 being the same as the new fp_hists
        assert all([aux["fp_hists"][i]["alert_mag"] == alert_mag for i in range(11)])
        assert all([aux["fp_hists"][i]["alert_mag"] == 15.0 for i in range(11, 21)])

        # verify they are still in order by jd (oldest to newest)
        assert all(
            [
                aux["fp_hists"][i]["jd"] < aux["fp_hists"][i + 1]["jd"]
                for i in range(len(aux["fp_hists"]) - 1)
            ]
        )

    def test_make_photometry(self):
        df_photometry = self.worker.make_photometry(self.alert)
        assert len(df_photometry) == 32

    def test_make_photometry_with_fp_hists(self):
        # here we ingest an alert with fp_hists, and verify that the photometry is correct
        candid = 2475433850015010009
        sample_avro = f"data/ztf_alerts/20231012/{candid}.avro"
        with open(sample_avro, "rb") as f:
            records = [record for record in fastavro.reader(f)]
            for record in records:
                post_alert(self.worker, record, fp_cutoff=1)

        df_photometry = self.worker.make_photometry(record, include_fp_hists=True)
        assert len(df_photometry) == 28
        # prv_candidates have origin = None, and fp_hists have origin = 'alert_fp'
        assert "origin" in df_photometry.columns
        assert len(df_photometry[df_photometry["origin"] == "alert_fp"]) == 7
        assert len(df_photometry[df_photometry["origin"].isnull()]) == 21
        # verify that they all have a fluxerr value
        assert all(df_photometry["fluxerr"].notnull())

        assert (
            len(
                df_photometry[
                    df_photometry["flux"].notnull() & df_photometry["origin"].isnull()
                ]
            )
            == 5
        )
        assert (
            len(
                df_photometry[
                    df_photometry["flux"].notnull()
                    & (df_photometry["origin"] == "alert_fp")
                ]
            )
            == 6
        )

    def test_make_thumbnails(self):
        alert, _, _ = self.worker.alert_mongify(self.alert)
        for ttype, istrument_type in [
            ("new", "Science"),
            ("ref", "Template"),
            ("sub", "Difference"),
        ]:
            thumb = self.worker.make_thumbnail(alert, ttype, istrument_type)
            assert len(thumb.get("data", [])) > 0

    def test_alert_filter__ml(self):
        """Test executing ML models on the alert"""
        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        all_prv_candidates = deepcopy(prv_candidates) + [deepcopy(alert["candidate"])]
        scores = self.worker.alert_filter__ml(alert, all_prv_candidates)
        assert len(scores) > 0
        # print(scores)

    def test_alert_filter__xmatch(self):
        """Test cross matching with external catalog"""
        alert, _, _ = self.worker.alert_mongify(self.alert)
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
            "radius": 1.0,
            "validity_days": 3,
        }
        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)

        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )
        assert passed_filters[0]["auto_followup"]["data"]["payload"]["priority"] == 3
        assert passed_filters[0]["auto_followup"]["data"]["radius"] == 1.0
        start_date = datetime.strptime(
            passed_filters[0]["auto_followup"]["data"]["payload"]["start_date"],
            "%Y-%m-%dT%H:%M:%S.%f",
        )
        end_date = datetime.strptime(
            passed_filters[0]["auto_followup"]["data"]["payload"]["end_date"],
            "%Y-%m-%dT%H:%M:%S.%f",
        )
        assert (end_date - start_date).days == 3

        # try the same but with a pipeline that overwrites the payload dynamically
        filter["auto_followup"]["pipeline"].append(
            {
                "$addFields": {
                    "payload.observation_type": "imaging",
                    "payload.priority": 0,
                    "comment": "Overwritten by pipeline",
                }
            }
        )

        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)
        delete_alert(self.worker, self.alert)

        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "imaging"
        )
        assert passed_filters[0]["auto_followup"]["data"]["payload"]["priority"] == 0
        assert (
            passed_filters[0]["auto_followup"]["comment"]
            == "Overwritten by pipeline (priority: 0)"
        )

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

        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(
            alert, prv_candidates, passed_filters=passed_filters
        )

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

        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(
            alert, prv_candidates, passed_filters=passed_filters
        )

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

        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(
            alert, prv_candidates, passed_filters=passed_filters
        )

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

        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(
            alert, prv_candidates, passed_filters=passed_filters
        )

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

        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(
            alert, prv_candidates, passed_filters=passed_filters
        )

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

        # verify that there is no follow-up request
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
                "unsaveGroupIds": [saved_group_id, target_group_id],
            },
        )
        assert response.status_code == 200

        # last but not least, we verify that we don't trigger the same request again, if a request has been submitted but then deleted, or is completed already
        filter = filter_template(self.worker.collection_alerts)
        filter["group_id"] = saved_group_id
        filter["autosave"] = {
            "active": True,
            "comment": "Saved to BTS by BTSbot.",
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
            "allocation_id": allocation_id,
            "payload": {  # example payload for SEDM
                "observation_type": "IFU",
                "priority": 2,
                "advanced": False,
            },
            "comment": "SEDM triggered by BTSbot",
        }

        passed_filters = self.worker.alert_filter__user_defined([filter], self.alert)
        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "auto_followup" in passed_filters[0]
        assert (
            passed_filters[0]["auto_followup"]["data"]["payload"]["observation_type"]
            == "IFU"
        )

        alert, prv_candidates, _ = self.worker.alert_mongify(self.alert)
        self.worker.alert_sentinel_skyportal(
            alert, prv_candidates, passed_filters=passed_filters
        )

        # now fetch the follow-up request from SP
        # we should not have any submitted follow-up requests
        response = self.worker.api_skyportal(
            "GET", f"/api/followup_request?sourceID={alert['objectId']}", None
        )
        assert response.status_code == 200
        followup_requests = response.json()["data"].get("followup_requests", [])
        submitted = [
            f
            for f in followup_requests
            if (f["allocation_id"] == allocation_id and f["status"] == "submitted")
        ]
        deleted = [
            f
            for f in followup_requests
            if (f["allocation_id"] == allocation_id and f["status"] == "deleted")
        ]
        assert len(submitted) == 0
        assert len(deleted) > 0

        # verify that it was save still
        response = self.worker.api_skyportal(
            "GET", f"/api/sources/{alert['objectId']}", None
        )
        assert response.status_code == 200
        source = response.json()["data"]
        assert source["id"] == "ZTF20aajcbhr"
        # should be saved to Program A and Sitewide Group, but not Program B
        assert any([g["id"] == saved_group_id for g in source["groups"]])
        assert not any([g["id"] == target_group_id for g in source["groups"]])
        assert not any([g["id"] == ignore_if_saved_group_id for g in source["groups"]])

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

    def test_alert_filter__user_defined__with_fp_hists(self):
        # user filter runs on an alert that has forced photometry
        # we verify:
        # - the filter can be applied to the alert
        # - the filter can make use of the forced photometry
        # - when saving to SkyPortal, the forced photometry is saved as well

        candid = 2475433850015010009
        sample_avro = f"data/ztf_alerts/20231012/{candid}.avro"
        with open(sample_avro, "rb") as f:
            records = [record for record in fastavro.reader(f)]
            for record in records:
                post_alert(self.worker, record, fp_cutoff=1)

        filter = filter_template(self.worker.collection_alerts, include_fp_hists=True)
        filter["autosave"] = True
        # we edit the pipeline to include a condition that uses the forced photometry
        # for reference, this condition checks if we have any detections in forced photomety
        # that are within 0.015 days of the alert, and have a SNR > 3
        # telling us that this is not a moving object + has been detected before
        # add a $project stage at the end of the pipeline
        filter["pipeline"].append(
            {
                "$project": {
                    "stationary_fp": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$fp_hists"}, "missing"]},
                            "then": False,
                            "else": {
                                "$anyElementTrue": {
                                    "$map": {
                                        "input": "$fp_hists",
                                        "as": "cand",
                                        "in": {
                                            "$and": [
                                                {
                                                    "$gt": [
                                                        {
                                                            "$abs": {
                                                                "$subtract": [
                                                                    "$t_now",
                                                                    "$$cand.jd",
                                                                ]
                                                            }
                                                        },
                                                        0.015,
                                                    ]
                                                },
                                                {"$gte": ["$$cand.snr", 3]},
                                            ]
                                        },
                                    }
                                }
                            },
                        }
                    }
                }
            }
        )

        # now filter on that condition being true, so add a $match stage at the end
        filter["pipeline"].append({"$match": {"stationary_fp": True}})

        passed_filters = self.worker.alert_filter__user_defined([filter], record)

        assert passed_filters is not None
        assert len(passed_filters) == 1
        assert "autosave" in passed_filters[0]

        alert, prv_candidates, fp_hists = self.worker.alert_mongify(record)

        # pop nulls from the fp_hists
        fp_hists = [
            {kk: vv for kk, vv in fp_hist.items() if vv not in [None, -99999, -99999.0]}
            for fp_hist in fp_hists
        ]

        # format the forced photometry
        fp_hists_formatted = self.worker.format_fp_hists(alert, fp_hists)

        # only run the rest if the broker mode is activated
        if config["misc"].get("broker", False) is False:
            pytest.skip("Broker mode not activated, skipping the rest of the test")

        self.worker.alert_sentinel_skyportal(
            alert,
            prv_candidates,
            fp_hists=fp_hists_formatted,
            passed_filters=passed_filters,
        )

        # verify that the forced photometry was saved to SkyPortal
        response = self.worker.api_skyportal(
            "GET", f"/api/sources/{record['objectId']}/photometry", None
        )

        assert response.status_code == 200
        photometry = response.json()["data"]
        assert len(photometry) == 28

        assert "origin" in photometry[0]
        assert len([p for p in photometry if p["origin"] == "alert_fp"]) == 7
        assert len([p for p in photometry if p["origin"] in [None, "None"]]) == 21

        assert (
            len(
                [
                    p
                    for p in photometry
                    if p["origin"] == "alert_fp" and p["mag"] is not None
                ]
            )
            == 3
        )
        assert (
            len(
                [
                    p
                    for p in photometry
                    if p["origin"] in [None, "None"] and p["mag"] is not None
                ]
            )
            == 2
        )

        fp_detection = [
            p for p in photometry if p["origin"] == "alert_fp" and p["mag"] is not None
        ][0]

        # we want to test the update of the forced photometry data.
        # To do so, modify the flux (flux+ 10) of all the detections in the forced photometry
        for i in range(len(fp_hists_formatted)):
            if fp_hists_formatted[i].get("forcediffimflux") is not None:
                fp_hists_formatted[i]["forcediffimflux"] += 50

        self.worker.alert_sentinel_skyportal(
            alert,
            prv_candidates,
            fp_hists=fp_hists_formatted,
            passed_filters=passed_filters,
        )

        # verify that the forced photometry was updated in SkyPortal
        # everything should be the same
        response = self.worker.api_skyportal(
            "GET", f"/api/sources/{record['objectId']}/photometry", None
        )

        assert response.status_code == 200
        photometry = response.json()["data"]
        assert len(photometry) == 28

        assert "origin" in photometry[0]
        assert len([p for p in photometry if p["origin"] == "alert_fp"]) == 7
        assert len([p for p in photometry if p["origin"] in [None, "None"]]) == 21

        assert (
            len(
                [
                    p
                    for p in photometry
                    if p["origin"] == "alert_fp" and p["mag"] is not None
                ]
            )
            == 3
        )

        assert (
            len(
                [
                    p
                    for p in photometry
                    if p["origin"] in [None, "None"] and p["mag"] is not None
                ]
            )
            == 2
        )

        updated_fp_detection = [
            p for p in photometry if p["origin"] == "alert_fp" and p["mag"] is not None
        ][0]

        assert updated_fp_detection["mag"] < fp_detection["mag"]
