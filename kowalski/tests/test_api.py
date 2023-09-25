import random
import string
from typing import List

import pytest
from kowalski.api.api import app_factory
from kowalski.utils import uid
from kowalski.config import load_config


config = load_config(config_files=["config.yaml"])["kowalski"]


async def get_admin_credentials(aiohttp_client):
    """
        Fixture to get authorization token for admin
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    _auth = await client.post(
        "/api/auth",
        json={
            "username": config["server"]["admin_username"],
            "password": config["server"]["admin_password"],
        },
    )

    credentials = await _auth.json()

    return credentials


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


def make_ztf_trigger(
    queue_name: str = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(9)
    ),
    validity_window_mjd: List = [random.random(), random.random() + 1],
    targets: List = [{}],
    queue_type: str = "list",
    user: str = "provisioned-admin",
):

    targets = [
        {
            "request_id": 1,
            "program_id": 2,
            "field_id": 699,
            "ra": 322.718872,
            "dec": 27.574113,
            "filter_id": 1,
            "exposure_time": 300.0,
            "program_pi": "Kulkarni/provisioned-admin",
            "subprogram_name": "ToO_GRB",
        },
        {
            "request_id": 2,
            "program_id": 2,
            "field_id": 700,
            "ra": 322.718872,
            "dec": 27.574113,
            "filter_id": 1,
            "exposure_time": 300.0,
            "program_pi": "Kulkarni/provisioned-admin",
            "subprogram_name": "ToO_GRB",
        },
    ]

    return {
        "queue_name": queue_name,
        "validity_window_mjd": validity_window_mjd,
        "targets": targets,
        "queue_type": queue_type,
        "user": user,
    }


def make_ztf_mma_trigger(
    target_name: str = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(9)
    ),
    trigger_time: float = random.random(),
    fields: List[dict] = [{}],
    user: str = "provisioned-admin",
):
    fields = [
        {"field_id": 550, "probability": 0.5},
        {"field_id": 650, "probability": 0.25},
    ]

    return {
        "trigger_name": target_name,
        "trigger_time": trigger_time,
        "fields": fields,
        "user": user,
    }


@pytest.mark.asyncio
async def test_auth(aiohttp_client):
    """
        Test authorization: /api/auth
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    _auth = await client.post(
        "/api/auth",
        json={
            "username": config["server"]["admin_username"],
            "password": config["server"]["admin_password"],
        },
    )
    assert _auth.status == 200

    credentials = await _auth.json()
    assert "token" in credentials


@pytest.mark.asyncio
async def test_auth_error(aiohttp_client):
    """Test authorization with invalid credentials: /api/auth

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    _auth = await client.post(
        "/api/auth", json={"username": "noname", "password": "nopass"}
    )
    assert _auth.status == 401

    credentials = await _auth.json()
    assert credentials["status"] == "error"
    assert credentials["message"] == "Unauthorized"


@pytest.mark.asyncio
async def test_users(aiohttp_client):
    """Test user management: /api/users

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # check JWT authorization
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    test_user = uid(6)
    test_user_edited = uid(6)
    test_user_password = uid(6)

    # adding a user
    resp = await client.post(
        "/api/users",
        json={"username": test_user, "password": test_user_password},
        headers=headers,
    )
    assert resp.status == 200

    # editing user data
    resp = await client.put(
        f"/api/users/{test_user}",
        json={"password": test_user_password},
        headers=headers,
    )
    assert resp.status == 200
    resp = await client.put(
        f"/api/users/{test_user}",
        json={"username": test_user_edited},
        headers=headers,
    )
    assert resp.status == 200

    # deleting a user
    resp = await client.delete(f"/api/users/{test_user_edited}", headers=headers)
    assert resp.status == 200


# test filters api


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
    # pipeline has been saved as a string, so we need to do the same before comparing
    auto_followup[
        "pipeline"
    ] = '[{"$match": {"candidate.drb": {"$gt": 0.9999}, "cross_matches.CLU_20190625.0": {"$exists": false}}}]'
    assert result["data"]["auto_followup"] == auto_followup

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


# test multiple query types without book-keeping (the default and almost exclusively used scenario):
#  - find_one
#  - find
#  - info
#  - count_documents
#  - estimated_document_count
#  - aggregate
#  - cone_search


@pytest.mark.asyncio
async def test_query_cone_search(aiohttp_client):
    """
        Test {"query_type": "cone_search", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    collection = "ZTF_alerts"

    # check query without book-keeping
    qu = {
        "query_type": "cone_search",
        "query": {
            "object_coordinates": {
                "cone_search_radius": 2,
                "cone_search_unit": "arcsec",
                "radec": {"object1": [71.6577756, -10.2263957]},
            },
            "catalogs": {
                collection: {
                    "filter": {},
                    "projection": {"_id": 0, "candid": 1, "objectId": 1},
                }
            },
        },
        "kwargs": {"filter_first": False},
    }
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result
    # should always return a dict, even if it's empty
    assert isinstance(result["data"], dict)


@pytest.mark.asyncio
async def test_query_find_one(aiohttp_client):
    """
        Test {"query_type": "find_one", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": access_token}

    collection = "ZTF_alerts"

    # check query without book-keeping
    qu = {
        "query_type": "find_one",
        "query": {
            "catalog": collection,
            "filter": {},
        },
        "kwargs": {"save": False},
    }
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    # print(result)
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result


@pytest.mark.asyncio
async def test_query_find(aiohttp_client):
    """
        Test {"query_type": "find", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": access_token}

    collection = "ZTF_alerts"

    # check query without book-keeping
    qu = {
        "query_type": "find",
        "query": {
            "catalog": collection,
            "filter": {"candid": {"$lt": 0}},
            "projection": {"_id": 0, "candid": 1},
        },
        "kwargs": {"save": False, "limit": 1},
    }
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    # print(result)
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result
    # should always return a list, even if it's empty
    assert isinstance(result["data"], list)


@pytest.mark.asyncio
async def test_query_aggregate(aiohttp_client):
    """
        Test {"query_type": "aggregate", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": access_token}

    collection = "ZTF_alerts"

    # check query without book-keeping
    qu = {
        "query_type": "aggregate",
        "query": {
            "catalog": collection,
            "pipeline": [
                {"$match": {"candid": 1127561445515015011}},
                {"$project": {"_id": 0, "candid": 1}},
            ],
        },
        "kwargs": {"max_time_ms": 2000},
    }
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    # print(result)
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result


@pytest.mark.asyncio
async def test_query_info(aiohttp_client):
    """
        Test {"query_type": "info", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": access_token}

    # check catalog_names info
    qu = {"query_type": "info", "query": {"command": "catalog_names"}}
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    # print(result)
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result


@pytest.mark.asyncio
async def test_query_count_documents(aiohttp_client):
    """
        Test {"query_type": "count_documents", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": access_token}

    collection = "ZTF_alerts"

    # check catalog_names info
    qu = {
        "query_type": "count_documents",
        "query": {"catalog": collection, "filter": {"candid": {"$lt": 0}}},
    }
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    # print(result)
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result
    assert result["data"] == 0


@pytest.mark.asyncio
async def test_query_estimated_document_count(aiohttp_client):
    """
        Test {"query_type": "estimated_document_count", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": access_token}

    collection = "ZTF_alerts"

    # check catalog_names info
    qu = {
        "query_type": "estimated_document_count",
        "query": {"catalog": collection},
    }
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    # print(result)
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result


@pytest.mark.asyncio
async def test_query_near(aiohttp_client):
    """
        Test {"query_type": "near", ...}: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    collection = "ZTF_alerts"

    # check query without book-keeping
    qu = {
        "query_type": "near",
        "query": {
            "min_distance": 0.1,
            "max_distance": 30,
            "distance_units": "arcsec",
            "radec": {"object1": [71.6577756, -10.2263957]},
            "catalogs": {
                collection: {
                    "filter": {},
                    "projection": {"_id": 0, "candid": 1, "objectId": 1},
                }
            },
        },
        "kwargs": {"filter_first": False},
    }
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["message"] == "Successfully executed query"
    assert "data" in result
    # should always return a dict, even if it's empty
    assert isinstance(result["data"], dict)


# test raising errors


@pytest.mark.asyncio
async def test_query_unauthorized(aiohttp_client):
    """
        Test an unauthorized query: POST /api/queries
    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    headers = {"Authorization": "no_token"}

    # check catalog_names info
    qu = {"query_type": "info", "query": {"command": "catalog_names"}}
    # print(qu)
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    # print(resp)
    assert resp.status == 400
    result = await resp.json()
    # print(result)
    assert result["status"] == "error"
    assert result["message"] == "token is invalid"


@pytest.mark.asyncio
async def test_triggers_ztf(aiohttp_client):
    """Test saving, testing, retrieving, modifying, and removing a ZTF trigger: /api/triggers/ztf

    :param aiohttp_client:
    :return:
    """

    client = await aiohttp_client(await app_factory())

    # authorize as admin, regular users cannot do this
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    ztf_trigger = make_ztf_trigger()

    # put
    resp = await client.put(
        "/api/triggers/ztf.test", json=ztf_trigger, headers=headers, timeout=5
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "message" in result

    # delete
    resp = await client.delete(
        "/api/triggers/ztf.test", json=ztf_trigger, headers=headers, timeout=5
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "message" in result


@pytest.mark.asyncio
async def test_mma_triggers_ztf(aiohttp_client):
    """Test saving, testing, retrieving, modifying, and removing a ZTF MMA trigger: /api/triggers/ztfmma

    :param aiohttp_client:
    :return:
    """
    client = await aiohttp_client(await app_factory())

    # authorize as admin, regular users cannot do this
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    ztf_mma_trigger = make_ztf_mma_trigger()

    # put
    resp = await client.put(
        "/api/triggers/ztfmma.test", json=ztf_mma_trigger, headers=headers, timeout=5
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "message" in result

    # delete
    resp = await client.delete(
        "/api/triggers/ztfmma.test", json=ztf_mma_trigger, headers=headers, timeout=5
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "message" in result


@pytest.mark.asyncio
async def test_skymap(aiohttp_client):
    """Test saving and querying skymaps"""
    client = await aiohttp_client(await app_factory())

    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    # get
    resp = await client.get(
        "/api/skymap",
        params={
            "dateobs": "2023-06-23T15:42:26",
            "localization_name": "90.00000_30.00000_10.00000",
            "contours": [90],
        },
        headers=headers,
        timeout=5,
    )
    if resp.status == 200:
        # delete
        resp = await client.delete(
            "/api/skymap",
            json={
                "dateobs": "2023-06-23T15:42:26",
                "localization_name": "90.00000_30.00000_10.00000",
            },
            headers=headers,
            timeout=5,
        )
        assert resp.status == 200

    # put
    resp = await client.put(
        "/api/skymap",
        json={
            "dateobs": "2023-06-23T15:42:26",
            "skymap": {
                "ra": 90,
                "dec": 30,
                "error": 10,
            },
            "contours": [90],
        },
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["data"]["dateobs"] == "2023-06-23T15:42:26"
    assert result["data"]["localization_name"] == "90.00000_30.00000_10.00000"
    assert result["data"]["contours"] == [90]

    # put update
    resp = await client.put(
        "/api/skymap",
        json={
            "dateobs": "2023-06-23T15:42:26",
            "skymap": {
                "ra": 90,
                "dec": 30,
                "error": 10,
            },
            "contours": [95],
        },
        headers=headers,
        timeout=5,
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["data"]["dateobs"] == "2023-06-23T15:42:26"
    assert result["data"]["localization_name"] == "90.00000_30.00000_10.00000"
    assert result["data"]["contours"] == [90, 95]

    # put already exists
    resp = await client.put(
        "/api/skymap",
        json={
            "dateobs": "2023-06-23T15:42:26",
            "skymap": {
                "ra": 90,
                "dec": 30,
                "error": 10,
            },
            "contours": [95],
        },
        headers=headers,
        timeout=5,
    )

    assert resp.status == 409
    result = await resp.json()
    assert result["status"] == "already_exists"
    assert result["data"]["dateobs"] == "2023-06-23T15:42:26"
    assert result["data"]["localization_name"] == "90.00000_30.00000_10.00000"
    assert result["data"]["contours"] == [90, 95]

    # get
    resp = await client.get(
        "/api/skymap",
        params={
            "dateobs": "2023-06-23T15:42:26",
            "localization_name": "90.00000_30.00000_10.00000",
            "contours": [90],
        },
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert result["data"]["dateobs"] == "2023-06-23T15:42:26"

    # query ztf alerts in the skymap
    qu = {
        "query_type": "skymap",
        "query": {
            "skymap": {
                "localization_name": "90.00000_30.00000_10.00000",
                "dateobs": "2023-06-23T15:42:26.000",
                "contour": 90,
            },
            "catalog": "ZTF_alerts",
            "filter": {},
            "projection": {"_id": 0, "objectId": 1},
        },
    }
    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert len(result["data"]) == 20

    # delete
    resp = await client.delete(
        "/api/skymap",
        json={
            "dateobs": "2023-06-23T15:42:26",
            "localization_name": "90.00000_30.00000_10.00000",
        },
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
