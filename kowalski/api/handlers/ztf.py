from abc import ABC
from typing import List, Optional

from astropy.time import Time
from aiohttp import ClientSession, web
from odmantic import Model
from sshtunnel import SSHTunnelForwarder

from kowalski.config import load_config

from kowalski.api.middlewares import (
    admin_required,
)
from .base import BaseHandler

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


class ZTFTriggerPut(Model, ABC):
    """Data model for ZTF trigger for streamlined validation"""

    queue_name: str
    validity_window_mjd: List[float]
    targets: List[dict]
    queue_type: str
    user: str


class ZTFTriggerGet(Model, ABC):
    """Data model for ZTF queue retrieval for streamlined validation"""

    queue_name: Optional[str]
    user: Optional[str]


class ZTFTriggerDelete(Model, ABC):
    """Data model for ZTF queue deletion for streamlined validation"""

    queue_name: str
    user: str


class ZTFTriggerHandler(BaseHandler):
    """Handlers to work with ZTF triggers"""

    def __init__(self, test: bool = False):
        """Constructor for ZTF trigger class

        :param test: is this a test trigger?
        :return:
        """

        self.test = test

    @admin_required
    async def get(self, request: web.Request) -> web.Response:
        """Retrieve ZTF queue

        :param request:
        :return:
        ---
        summary: Get ZTF queue
        tags:
          - triggers

        requestBody:
          required: false
          content:
            application/json:
              schema:
                type: object
        responses:
          '200':
            description: queue retrieved
            content:
              application/json:
                schema:
                  type: object
          '400':
            description: query parsing/execution error
            content:
              application/json:
                schema:
                  type: object
        """
        try:
            _data = await request.json()
        except Exception:
            _data = {}
            trigger_name = request.query.get("trigger_name", None)
            user = request.query.get("user", None)
            if trigger_name:
                _data["trigger_name"] = trigger_name
            if user:
                _data["user"] = user

        # validate
        ZTFTriggerGet(**_data)
        if self.test:
            return self.success(message="submitted", data=[{"queue_name": "test"}])

        server = SSHTunnelForwarder(
            (config["ztf"]["mountain_ip"], config["ztf"]["mountain_port"]),
            ssh_username=config["ztf"]["mountain_username"],
            ssh_password=config["ztf"]["mountain_password"],
            remote_bind_address=(
                config["ztf"]["mountain_bind_ip"],
                config["ztf"]["mountain_bind_port"],
            ),
        )

        server.start()
        url = f"http://{server.local_bind_address[0]}:{server.local_bind_address[1]}/queues"
        async with ClientSession() as client_session:
            async with client_session.get(url, json=_data, timeout=10) as response:
                response_json = await response.json()
        server.stop()

        if response.status == 200:
            return self.success(message="retrieved", data=response_json)
        return self.error(message=f"ZTF queue query attempt rejected: {response.text}")

    @admin_required
    async def put(self, request: web.Request) -> web.Response:
        """Trigger ZTF

        :param request:
        :return:
        ---
        summary: Trigger ZTF
        tags:
          - triggers

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
        responses:
          '201':
            description: queue submitted
            content:
              application/json:
                schema:
                  type: object
          '200':
            description: queue already exists
            content:
              application/json:
                schema:
                  type: object
          '400':
            description: query parsing/execution error
            content:
              application/json:
                schema:
                  type: object
        """

        _data = await request.json()

        # validate
        ZTFTriggerPut(**_data)

        if self.test:
            return self.success(message="submitted")

        server = SSHTunnelForwarder(
            (config["ztf"]["mountain_ip"], config["ztf"]["mountain_port"]),
            ssh_username=config["ztf"]["mountain_username"],
            ssh_password=config["ztf"]["mountain_password"],
            remote_bind_address=(
                config["ztf"]["mountain_bind_ip"],
                config["ztf"]["mountain_bind_port"],
            ),
        )

        server.start()
        url = f"http://{server.local_bind_address[0]}:{server.local_bind_address[1]}/queues"
        async with ClientSession() as client_session:
            response = await client_session.put(url, json=_data, timeout=10)
        server.stop()

        if response.status == 201:
            return self.success(message="submitted", data=dict(response.headers))

        elif response.status == 200:
            data = dict(response.headers)
            return self.error(
                message=f"Submitted queue {data['queue_name']} already exists",
                status=409,
            )

        return self.error(message=f"ZTF trigger attempt rejected: {response.text}")

    @admin_required
    async def delete(self, request: web.Request) -> web.Response:
        """Delete ZTF request

        :param request:
        :return:
        ---
        summary: Delete ZTF request
        tags:
          - triggers

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
        responses:
          '200':
            description: queue removed
            content:
              application/json:
                schema:
                  type: object
          '400':
            description: query parsing/execution error
            content:
              application/json:
                schema:
                  type: object
        """

        _data = await request.json()

        # validate and preprocess
        ZTFTriggerDelete(**_data)

        if self.test:
            return self.success(message="deleted")

        server = SSHTunnelForwarder(
            (config["ztf"]["mountain_ip"], config["ztf"]["mountain_port"]),
            ssh_username=config["ztf"]["mountain_username"],
            ssh_password=config["ztf"]["mountain_password"],
            remote_bind_address=(
                config["ztf"]["mountain_bind_ip"],
                config["ztf"]["mountain_bind_port"],
            ),
        )

        server.start()
        url = f"http://{server.local_bind_address[0]}:{server.local_bind_address[1]}/queues"
        async with ClientSession() as client_session:
            response = await client_session.delete(url, json=_data, timeout=10)
        server.stop()

        if response.status == 200:
            return self.success(message="deleted", data=dict(response.headers))
        return self.error(message=f"ZTF queue delete attempt rejected: {response.text}")


class ZTFMMATriggerPut(Model, ABC):
    """Data model for ZTF MMA trigger for streamlined validation"""

    trigger_name: str
    trigger_time: float
    fields: List[dict]
    user: str


class ZTFMMATriggerGet(Model, ABC):
    """Data model for ZTF MMA trigger retrieval for streamlined validation"""

    trigger_name: Optional[str]
    user: Optional[str]


class ZTFMMATriggerDelete(Model, ABC):

    trigger_name: str
    user: str


class ZTFMMATriggerHandler(BaseHandler):
    """Handlers to work with ZTF triggers"""

    def __init__(self, test: bool = False):
        """Constructor for ZTF MMA trigger class

        :param test: is this a test MMA trigger?
        :return:
        """

        self.test = test

    @admin_required
    async def get(self, request: web.Request) -> web.Response:
        """Retrieve ZTF MMA trigger(s) status

        :param request:
        :return:
        ---
        summary: Get ZTF MMA trigger(s) status
        tags:
          - triggers

        requestBody:
          required: false
          content:
            application/json:
              schema:
                type: object
        responses:
          '200':
            description: mma trigger(s) status retrieved
            content:
              application/json:
                schema:
                  type: object
          '400':
            description: mma trigger query parsing/execution error
            content:
              application/json:
                schema:
                  type: object
        """

        try:
            _data = await request.json()
        except Exception:
            _data = {}
            trigger_name = request.query.get("trigger_name", None)
            user = request.query.get("user", None)
            if trigger_name:
                _data["trigger_name"] = trigger_name
            if user:
                _data["user"] = user

        # validate
        ZTFMMATriggerGet(**_data)

        if self.test:
            return self.success(message="submitted", data=[{"trigger_name": "test"}])

        server = SSHTunnelForwarder(
            (config["ztf"]["mountain_ip"], config["ztf"]["mountain_port"]),
            ssh_username=config["ztf"]["mountain_username"],
            ssh_password=config["ztf"]["mountain_password"],
            remote_bind_address=(
                config["ztf"]["mountain_bind_ip"],
                config["ztf"]["mountain_bind_port"],
            ),
        )

        server.start()
        url = f"http://{server.local_bind_address[0]}:{server.local_bind_address[1]}/mma_skymaps"
        async with ClientSession() as client_session:
            async with client_session.get(url, json=_data, timeout=10) as response:
                response_json = await response.json()
        server.stop()

        if response.status == 200:
            return self.success(message="retrieved", data=response_json)
        return self.error(
            message=f"ZTF MMA trigger status query attempt rejected: {response.text}"
        )

    @admin_required
    async def put(self, request: web.Request) -> web.Response:
        """Trigger ZTF

        :param request:
        :return:
        ---
        summary: Trigger ZTF
        tags:
          - triggers

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
        responses:
          '201':
            description: trigger submitted
            content:
              application/json:
                schema:
                  type: object
          '200':
            description: trigger already exists
            content:
              application/json:
                schema:
                  type: object
          '400':
            description: trigger parsing/execution error
            content:
              application/json:
                schema:
                  type: object
        """

        _data = await request.json()

        # validate
        ZTFMMATriggerPut(**_data)

        if self.test:
            return self.success(message="submitted")

        server = SSHTunnelForwarder(
            (config["ztf"]["mountain_ip"], config["ztf"]["mountain_port"]),
            ssh_username=config["ztf"]["mountain_username"],
            ssh_password=config["ztf"]["mountain_password"],
            remote_bind_address=(
                config["ztf"]["mountain_bind_ip"],
                config["ztf"]["mountain_bind_port"],
            ),
        )

        server.start()
        url = f"http://{server.local_bind_address[0]}:{server.local_bind_address[1]}/mma_skymaps"
        async with ClientSession() as client_session:
            response = await client_session.put(url, json=_data, timeout=10)
        server.stop()

        if response.status == 201:
            return self.success(message="submitted", data=dict(response.headers))

        elif response.status == 200:
            data = dict(response.headers)
            return self.error(
                message=f"Submitted trigger {data['trigger_name']} already exists",
                status=409,
            )

        return self.error(
            message=f"ZTF MMA trigger submission attempt rejected: {response.text}"
        )

    @admin_required
    async def delete(self, request: web.Request) -> web.Response:
        """Delete ZTF request

        :param request:
        :return:
        ---
        summary: Delete ZTF request
        tags:
          - triggers

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
        responses:
          '200':
            description: queue removed
            content:
              application/json:
                schema:
                  type: object
          '400':
            description: query parsing/execution error
            content:
              application/json:
                schema:
                  type: object
        """

        _data = await request.json()

        # validate and preprocess
        ZTFMMATriggerDelete(**_data)

        if self.test:
            return self.success(message="deleted")

        server = SSHTunnelForwarder(
            (config["ztf"]["mountain_ip"], config["ztf"]["mountain_port"]),
            ssh_username=config["ztf"]["mountain_username"],
            ssh_password=config["ztf"]["mountain_password"],
            remote_bind_address=(
                config["ztf"]["mountain_bind_ip"],
                config["ztf"]["mountain_bind_port"],
            ),
        )

        server.start()
        url = f"http://{server.local_bind_address[0]}:{server.local_bind_address[1]}/mma_skymaps"
        async with ClientSession() as client_session:
            response = await client_session.delete(url, json=_data, timeout=10)
        server.stop()

        if response.status == 200:
            return self.success(message="deleted", data=dict(response.headers))
        return self.error(
            message=f"ZTF MMA trigger delete attempt rejected: {response.text}"
        )


class ZTFObsHistoryGet(Model, ABC):
    """Data model for ZTF Observation History for streamlined validation"""

    # TODO: add validation for date format, and obtain the name of the date parameter.
    # For now, we just know that it needs to be an astropy-time-compatible string (‘2018-01-01’)

    date: Optional[
        str
    ]  # if no date is provide, it will return observation of the current date


class ZTFObsHistoryHandler(BaseHandler):
    """ZTF Observation History Handler"""

    @admin_required
    async def get(self, request: web.Request) -> web.Response:
        """Get ZTF Observation History

        :param request:
        :return:
        ---
        summary: Get ZTF Observation History

        parameters:
          - in: query
            name: date
            description: "date of observation history"
            required: false
            schema:
              type: string

        responses:
          '200':
            description: retrieved
            content:
              application/json:
                schema:
                  type: object
          '400':
            description: query parsing/execution error
            content:
              application/json:
                schema:
                  type: object
        """

        _data = request.query

        # validate and preprocess
        ZTFObsHistoryGet(**_data)
        # if data has a date, verify that it can be parsed by astropy.time.Time
        if "date" in _data:
            try:
                Time(_data["date"])
            except Exception as e:
                return self.error(
                    message=f"ZTF Observation History date parsing {_data['date']} error:{str(e)}"
                )

        if self.test:
            return self.success(message="retrieved")

        server = SSHTunnelForwarder(
            (config["ztf"]["mountain_ip"], config["ztf"]["mountain_port"]),
            ssh_username=config["ztf"]["mountain_username"],
            ssh_password=config["ztf"]["mountain_password"],
            remote_bind_address=(
                config["ztf"]["mountain_bind_ip"],
                config["ztf"]["mountain_bind_port"],
            ),
        )

        server.start()
        url = f"http://{server.local_bind_address[0]}:{server.local_bind_address[1]}/obs_history"
        async with ClientSession() as client_session:
            async with client_session.get(url, params=_data, timeout=10) as response:
                response_json = await response.json()
        server.stop()

        if response.status == 200:
            return self.success(message="retrieved", data=response_json)
        return self.error(
            message=f"ZTF Observation History query attempt rejected: {response.text}"
        )
