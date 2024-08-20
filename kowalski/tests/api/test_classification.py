import pytest
from kowalski.api.api import app_factory
from kowalski.config import load_config

from kowalski.tests.api.test_user import get_admin_credentials


config = load_config(config_files=["config.yaml"])["kowalski"]


@pytest.mark.asyncio
async def test_alert_classification(aiohttp_client):
    """Test saving, testing, retrieving, modifying, and removing an alert classification: /api/alerts/classifications

    :param aiohttp_client:
    :return:
    """

    client = await aiohttp_client(await app_factory())

    # authorize as admin, regular users cannot do this
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    # first query with a find (with limit 1) to get any alert from the database
    qu = {
        "query_type": "find",
        "query": {
            "catalog": "ZTF_alerts",
            "filter": {},
            "projection": {"_id": 0, "candid": 1, "objectId": 1},
        },
        "kwargs": {"save": False, "limit": 1},
    }

    resp = await client.post("/api/queries", json=qu, headers=headers, timeout=5)
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert isinstance(result["data"], list)
    assert len(result["data"]) == 1
    candid = result["data"][0]["candid"]
    object_id = result["data"][0]["objectId"]

    # put classification(s)
    labels = {
        "Ia": 0.8,
        "Ib": 0.1,
    }
    resp = await client.put(
        "/api/classifications/ztf",
        json={
            candid: {
                "human_labels": labels,
            }
        },
        headers=headers,
        timeout=5,
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"

    # get classification(s)
    resp = await client.get(
        f"/api/classifications/ztf/{candid}",
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "candid" in result["data"]
    assert "objectId" in result["data"]
    assert "human_labels" in result["data"]
    assert result["data"]["candid"] == candid
    assert result["data"]["objectId"] == object_id
    assert len(result["data"]["human_labels"]) == 2
    assert result["data"]["human_labels"]["Ia"] == 0.8
    assert result["data"]["human_labels"]["Ib"] == 0.1

    # now, add a new label and overwrite an existing one
    labels = {
        "Ia": 0.2,
        "Ic": 0.7,
    }

    resp = await client.put(
        "/api/classifications/ztf",
        json={
            candid: {
                "human_labels": labels,
            }
        },
        headers=headers,
        timeout=5,
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"

    # get classification(s)
    resp = await client.get(
        f"/api/classifications/ztf/{candid}",
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "candid" in result["data"]
    assert "objectId" in result["data"]
    assert "human_labels" in result["data"]
    assert result["data"]["candid"] == candid
    assert result["data"]["objectId"] == object_id
    assert len(result["data"]["human_labels"]) == 3
    assert result["data"]["human_labels"]["Ia"] == 0.2
    assert result["data"]["human_labels"]["Ib"] == 0.1
    assert result["data"]["human_labels"]["Ic"] == 0.7

    # delete classification(s)
    # remove the Ia and Ic labels

    resp = await client.delete(
        "/api/classifications/ztf",
        json={
            candid: {
                "human_labels": ["Ia", "Ic"],
            }
        },
        headers=headers,
        timeout=5,
    )

    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"

    # get classification(s)
    resp = await client.get(
        f"/api/classifications/ztf/{candid}",
        headers=headers,
        timeout=5,
    )
    assert resp.status == 200
    result = await resp.json()
    assert result["status"] == "success"
    assert "data" in result
    assert "candid" in result["data"]
    assert "objectId" in result["data"]
    assert "human_labels" in result["data"]
    assert result["data"]["candid"] == candid
    assert result["data"]["objectId"] == object_id
    assert len(result["data"]["human_labels"]) == 1
    assert result["data"]["human_labels"]["Ib"] == 0.1
    assert "Ia" not in result["data"]["human_labels"]
    assert "Ic" not in result["data"]["human_labels"]
