import random
import string
from typing import List

import pytest

from kowalski.api.api import app_factory
from kowalski.config import load_config

from kowalski.tests.api.test_user import get_admin_credentials

config = load_config(config_files=["config.yaml"])["kowalski"]


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
