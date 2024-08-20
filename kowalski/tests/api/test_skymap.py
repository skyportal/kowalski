import pytest

from kowalski.api.api import app_factory
from kowalski.config import load_config

from kowalski.tests.api.test_user import get_admin_credentials


config = load_config(config_files=["config.yaml"])["kowalski"]


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
