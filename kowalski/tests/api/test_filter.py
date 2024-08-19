import random
from typing import List

import pytest

from kowalski.api.api import app_factory
from kowalski.config import load_config

from kowalski.tests.api.test_user import get_admin_credentials


config = load_config(config_files=["config.yaml"])["kowalski"]


def make_filter(
    filter_id: int = random.randint(1, 1000),
    group_id: int = random.randint(1, 1000),
    collection: str = "ZTF_alerts",
    permissions: List = None,
    pipeline: List = None,
):
    if permissions is None:
        permissions = [1, 2]

    if pipeline is None:
        pipeline = [
            {
                "$match": {
                    "candidate.drb": {"$gt": 0.9999},
                    "cross_matches.CLU_20190625.0": {"$exists": False},
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

    return {
        "group_id": group_id,
        "filter_id": filter_id,
        "catalog": collection,
        "permissions": permissions,
        "pipeline": pipeline,
    }


@pytest.mark.asyncio
async def test_filters(aiohttp_client):
    """Test saving, testing, retrieving, modifying, and removing a user-defined filter: /api/filters

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize as admin, regular users cannot do this
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    filter_id = random.randint(1, 10000)

    user_filter = make_filter(filter_id=filter_id)

    # post:
    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "fid" in result["data"]
    fid1 = result["data"]["fid"]

    # retrieve
    resp = await client.get(f"/api/filters/{filter_id}", headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["message"] == f"Retrieved filter id {filter_id}"
    assert "data" in result
    assert "active_fid" in result["data"]
    assert result["data"]["active_fid"] == fid1
    assert "autosave" in result["data"]
    assert result["data"]["autosave"] is False
    assert "auto_followup" in result["data"]
    assert result["data"]["auto_followup"] == {}

    # post new version:
    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "fid" in result["data"]
    fid2 = result["data"]["fid"]

    # generated new id?
    assert fid2 != fid1

    # retrieve again
    resp = await client.get(f"/api/filters/{filter_id}", headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["message"] == f"Retrieved filter id {filter_id}"
    assert "data" in result
    assert "active_fid" in result["data"]
    assert result["data"]["active_fid"] == fid2

    # make first version active
    resp = await client.patch(
        "/api/filters",
        json={"filter_id": filter_id, "active_fid": fid1},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "active_fid" in result["data"]
    assert result["data"]["active_fid"] == fid1

    # turn autosave on
    resp = await client.patch(
        "/api/filters",
        json={"filter_id": filter_id, "autosave": True},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "autosave" in result["data"]
    assert result["data"]["autosave"]

    # turn complex autosave on
    autosave = {
        "active": True,
        "comment": "test autosave",
        # we also use this to test how well a string pipeline is handled
        "pipeline": '[{"$match": {"candidate.drb": {"$gt": 0.9999}, "cross_matches.CLU_20190625.0": {"$exists": false}}}]',
        "ignore_group_ids": [1, 2, 3],
    }
    resp = await client.patch(
        "/api/filters",
        json={
            "filter_id": filter_id,
            "autosave": autosave,
        },
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "autosave" in result["data"]
    assert result["data"]["autosave"] == autosave

    # turn auto_followup on
    auto_followup = {
        "active": True,
        "allocation_id": 1,
        "comment": "test auto_followup",
        "priority_order": "desc",
        "payload": {  # example payload for SEDM
            "observation_type": "IFU",
        },
        "pipeline": [
            {
                "$match": {
                    "candidate.drb": {"$gt": 0.9999},
                    "cross_matches.CLU_20190625.0": {"$exists": False},
                }
            },
        ],
    }
    resp = await client.patch(
        "/api/filters",
        json={"filter_id": filter_id, "auto_followup": auto_followup},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "auto_followup" in result["data"]
    assert "priority_order" in result["data"]["auto_followup"]
    assert result["data"]["auto_followup"]["priority_order"] == "desc"
    # pipeline has been saved as a string, so we need to do the same before comparing
    auto_followup[
        "pipeline"
    ] = '[{"$match": {"candidate.drb": {"$gt": 0.9999}, "cross_matches.CLU_20190625.0": {"$exists": false}}}]'
    assert result["data"]["auto_followup"] == auto_followup

    # verify that we can't post another filter with the same allocation_id but a different priority_order "asc"
    asc_filter = make_filter(filter_id=filter_id)
    asc_filter["autosave"] = autosave
    asc_filter["auto_followup"] = {
        "active": True,
        "allocation_id": 1,
        "comment": "test auto_followup asc",
        "priority_order": "asc",
        "payload": {  # example payload for SEDM
            "observation_type": "IFU",
        },
        "pipeline": [
            {
                "$match": {
                    "candidate.drb": {"$gt": 0.9999},
                    "cross_matches.CLU_20190625.0": {"$exists": False},
                }
            },
        ],
    }
    resp = await client.post(
        "/api/filters", json=asc_filter, headers=headers, timeout=5
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"
    assert "message" in result
    assert (
        "filters with the same allocation have priority_order set to a different value, which is unexpected"
        in result["message"]
    )

    # turn update_annotations on
    resp = await client.patch(
        "/api/filters",
        json={
            "filter_id": filter_id,
            "update_annotations": True,
        },
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "update_annotations" in result["data"]
    assert result["data"]["update_annotations"]

    # deactivate
    resp = await client.patch(
        "/api/filters",
        json={"filter_id": filter_id, "active": False},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "active" in result["data"]
    assert not result["data"]["active"]

    # retrieve again
    resp = await client.get(f"/api/filters/{filter_id}", headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["message"] == f"Retrieved filter id {filter_id}"
    assert "data" in result
    assert "active" in result["data"]
    assert not result["data"]["active"]

    # remove filter
    resp = await client.delete(
        f"/api/filters/{filter_id}",
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["message"] == f"Removed filter id {filter_id}"

    # try adding a brand new filter, but already with autosave and auto_followup
    # (this is how the frontend will do it)
    user_filter = make_filter(filter_id=filter_id)
    user_filter["autosave"] = autosave
    user_filter["auto_followup"] = auto_followup
    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"


# test raising errors


@pytest.mark.asyncio
async def test_invalid_pipeline_stage_in_filter(aiohttp_client):
    """Test trying to save a bad filter with an invalid pipeline stage: POST /api/filters

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    pipeline = [
        {
            "$matc": {  # <- should be "$match"
                "candidate.drb": {"$gt": 0.9999},
                "cross_matches.CLU_20190625.0": {"$exists": False},
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

    user_filter = make_filter(pipeline=pipeline)

    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"


@pytest.mark.asyncio
async def test_forbidden_pipeline_stage_in_filter(aiohttp_client):
    """Test trying to save a bad filter with an invalid stage: POST /api/filters

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    pipeline = [
        {
            "$match": {
                "candidate.drb": {"$gt": 0.9999},
                "cross_matches.CLU_20190625.0": {"$exists": False},
            }
        },
        {
            "$addFields": {
                "annotations.author": "dd",
                "annotations.mean_rb": {"$avg": "$prv_candidates.rb"},
            }
        },
        {
            "$lookup": {  # <- $lookup is not allowed
                "from": "ZTF_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux",
            }
        },
        {"$project": {"_id": 0, "candid": 1, "objectId": 1, "annotations": 1}},
    ]
    user_filter = make_filter(pipeline=pipeline)

    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"
    assert "message" in result
    assert "pipeline uses forbidden stages" in result["message"]


@pytest.mark.asyncio
async def test_set_nonexistent_active_fid(aiohttp_client):
    """
    Test trying to set an invalid active filter version: PATCH /api/filters

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    filter_id = random.randint(1, 1000)

    user_filter = make_filter(filter_id=filter_id)

    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"

    # Try making fake version active
    resp = await client.patch(
        "/api/filters",
        json={"filter_id": filter_id, "active_fid": "somerandomfid"},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"
    assert "message" in result
    assert "filter version fid not in filter" in result["message"]

    # clean up: remove posted filter
    resp = await client.delete(
        f"/api/filters/{filter_id}",
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["message"] == f"Removed filter id {filter_id}"


@pytest.mark.asyncio
async def test_patch_remove_nonexistent_filter(aiohttp_client):
    """Test trying to patch and remove a non-existent filter:
    PATCH /api/filters
    DELETE /api/filters/{filter_id}

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    filter_id = random.randint(1, 1000)

    # Try patching a non-existent filter
    resp = await client.patch(
        "/api/filters",
        json={
            "filter_id": filter_id,
            "update_annotations": True,
        },
        headers=headers,
        timeout=5,
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"
    assert "message" in result
    assert f"Filter id {filter_id} not found" in result["message"]

    # Try removing a non-existent filter
    resp = await client.delete(
        f"/api/filters/{filter_id}",
        headers=headers,
        timeout=5,
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"
    assert "message" in result
    assert f"Filter id {filter_id} not found" in result["message"]


@pytest.mark.asyncio
async def test_filter_test_handler(aiohttp_client):
    # test the filter test handler, that allows users to run
    # a given filter they've saved on a given list of objects or between 2 jd dates
    # it's a POST request to /api/filters/filter_id/test
    # that takes an optional list of object ids and/or start_date and end_date
    # and returns the list of alerts that pass the filter

    client = await aiohttp_client(await app_factory())

    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    filter_id = random.randint(1, 1000)

    user_filter = make_filter(filter_id=filter_id)

    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 200
    result = await resp.json()

    assert result["status"] == "success"

    # test the filter test handler
    # first, test with a list of object ids
    object_ids = ["ZTF17aadcvhq"]
    resp = await client.post(
        f"/api/filters/{filter_id}/test",
        json={"object_ids": object_ids},
        headers=headers,
        timeout=5,
    )
    print(resp)
    print(await resp.text())

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "message" in result

    assert isinstance(result["data"], list)
    assert len(result["data"]) == 0

    # post a new version of the filter where the drb threshold is lower (0.5)
    user_filter["pipeline"][0]["$match"]["candidate.drb"] = {"$gt": 0.5}
    resp = await client.post(
        "/api/filters", json=user_filter, headers=headers, timeout=5
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"

    # test the filter test handler again
    # first, test with a list of object ids
    object_ids = ["ZTF17aadcvhq"]
    resp = await client.post(
        f"/api/filters/{filter_id}/test",
        json={"object_ids": object_ids},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "message" in result

    assert isinstance(result["data"], list)
    assert len(result["data"]) == 1

    # now try it with a start and end date
    start_date = 2460229.9338542
    end_date = start_date + 1
    resp = await client.post(
        f"/api/filters/{filter_id}/test",
        json={"start_date": start_date, "end_date": end_date},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "message" in result

    assert isinstance(result["data"], list)
    assert len(result["data"]) == 1

    # now try it with a start and end date that don't match any alerts
    start_date = 2460229.9338542 - 5000
    end_date = start_date + 1
    resp = await client.post(
        f"/api/filters/{filter_id}/test",
        json={"start_date": start_date, "end_date": end_date},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "message" in result

    assert isinstance(result["data"], list)
    assert len(result["data"]) == 0

    # now try it with an invalid time range (more than 1 jd)
    start_date = 2460229.9338542
    end_date = start_date + 2
    resp = await client.post(
        f"/api/filters/{filter_id}/test",
        json={"start_date": start_date, "end_date": end_date},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"
    assert "message" in result
    assert (
        "start_date and end_date must be within 1 day of each other"
        in result["message"]
    )

    # now try both object ids and a time range to see that we can now use more than one jd
    # without getting an error
    start_date = 2460229.9338542
    end_date = start_date + 30
    resp = await client.post(
        f"/api/filters/{filter_id}/test",
        json={"object_ids": object_ids, "start_date": start_date, "end_date": end_date},
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"

    assert isinstance(result["data"], list)
    assert len(result["data"]) == 1

    # last but not least, check that we can't pass an max_time_ms > 3000000 (5 minutes)
    resp = await client.post(
        f"/api/filters/{filter_id}/test",
        json={
            "object_ids": object_ids,
            "start_date": start_date,
            "end_date": end_date,
            "max_time_ms": 3000001,
        },
        headers=headers,
        timeout=5,
    )
    assert resp.status == 400
    result = await resp.json()
    assert result["status"] == "error"
    assert "message" in result
    assert (
        "max_time_ms must be less than 5 minutes (in ms: 300000)" in result["message"]
    )

    # clean up: remove posted filter
    resp = await client.delete(
        f"/api/filters/{filter_id}",
        headers=headers,
        timeout=5,
    )
