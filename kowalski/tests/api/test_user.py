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


@pytest.mark.asyncio
async def test_ping(aiohttp_client):
    client = await aiohttp_client(await app_factory())

    # check JWT authorization
    credentials = await get_admin_credentials(aiohttp_client)
    access_token = credentials["token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    _ping = await client.get(
        "/",
        headers=headers,
    )
    assert _ping.status == 200

    ping = await _ping.json()
    assert ping["status"] == "success"
    assert ping["message"] == "greetings from Kowalski!"


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
