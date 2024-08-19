import pytest

from kowalski.api.api import app_factory
from kowalski.config import load_config

from kowalski.tests.api.test_user import get_admin_credentials


config = load_config(config_files=["config.yaml"])["kowalski"]

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
