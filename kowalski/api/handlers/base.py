from typing import Mapping, Optional

from aiohttp import web
from bson.json_util import dumps

from kowalski.config import load_config

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


class BaseHandler:
    @staticmethod
    def success(message: str = "", data: Optional[Mapping] = None):
        response = {"status": "success", "message": message}
        if data is not None:
            response["data"] = data
        return web.json_response(response, status=200, dumps=dumps)

    @staticmethod
    def error(message: str = "", status: int = 400):
        return web.json_response({"status": "error", "message": message}, status=status)


def is_admin(username: str):
    """Check if user is admin
    note: may want to change the logic to allow multiple users to be admins

    :param username:
    """
    return username == config["server"]["admin_username"]
