import datetime
import gzip
import io
import os
import pathlib
import traceback
from abc import ABC
from ast import literal_eval
from typing import Any, List, Mapping, Optional, Sequence, Union

import jwt
import lxml
import matplotlib.pyplot as plt
import numpy as np
import uvloop
import xmlschema
from aiohttp import ClientSession, web
from aiohttp_swagger3 import ReDocUiSettings, SwaggerDocs
from astropy.io import fits
from astropy.time import Time
from astropy.visualization import (
    AsinhStretch,
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
    MinMaxInterval,
    SqrtStretch,
    ZScaleInterval,
)
from bson.json_util import dumps, loads
from matplotlib.colors import LogNorm
from motor.motor_asyncio import AsyncIOMotorClient
from multidict import MultiDict
from odmantic import AIOEngine, EmbeddedModel, Field, Model
from pydantic import ValidationError, root_validator
from sshtunnel import SSHTunnelForwarder

from kowalski.api.middlewares import (
    admin_required,
    auth_middleware,
    auth_required,
    error_middleware,
)
from kowalski.config import load_config
from kowalski.log import log
from kowalski.tools.gcn_utils import (
    from_dict,
    from_voevent,
    get_aliases,
    get_contours,
    get_dateobs,
    get_trigger,
)
from kowalski.utils import (
    add_admin,
    check_password_hash,
    generate_password_hash,
    init_db,
    radec_str2geojson,
    uid,
    str_to_numeric,
)

voevent_schema = xmlschema.XMLSchema("schema/VOEvent-v2.0.xsd")

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

# dict with the keys allowed in an filter's autosave section, and their data types
AUTOSAVE_KEYS = {
    "active": bool,
    "comment": str,
    "ignore_group_ids": list,
    "pipeline": list,
}

# dict with the keys allowed in an filter's auto_followup section, and their data types
AUTO_FOLLOWUP_KEYS = {
    "active": bool,
    "comment": str,
    "pipeline": list,
    "allocation_id": str,
    "payload": dict,
    "target_group_ids": list,
    "radius": float,
    "validity_days": int,
    "priority_order": str,
}


class Handler:
    @staticmethod
    def success(message: str = "", data: Optional[Mapping] = None):
        response = {"status": "success", "message": message}
        if data is not None:
            response["data"] = data
        return web.json_response(response, status=200, dumps=dumps)

    @staticmethod
    def error(message: str = "", status: int = 400):
        return web.json_response({"status": "error", "message": message}, status=status)


""" authentication and authorization """


def is_admin(username: str):
    """Check if user is admin
    note: may want to change the logic to allow multiple users to be admins

    :param username:
    """
    return username == config["server"]["admin_username"]


# @routes.post('/api/auth')
async def auth_post(request: web.Request) -> web.Response:
    """
    Authentication

    ---
    summary: Get access token
    tags:
      - auth

    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            required:
              - username
              - password
            properties:
              username:
                type: string
              password:
                type: string
          example:
            username: user
            password: PwD

    responses:
      '200':
        description: access token
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - token
              properties:
                status:
                  type: string
                token:
                  type: string
            example:
              status: success
              token: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiYWRtaW4iLCJleHAiOjE1OTE1NjE5MTl9.2emEp9EKf154WLJQwulofvXhTX7L0s9Y2-6_xI0Gx8w

      '400':
        description: username or password missing in requestBody
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                message:
                  type: string
            examples:
              missing username:
                value:
                  status: error
                  message: missing username
              missing password:
                value:
                  status: error
                  message: missing password

      '401':
        description: bad credentials
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                message:
                  type: string
            example:
              status: error
              message: wrong credentials

      '500':
        description: internal/unknown cause of failure
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                message:
                  type: string
            example:
              status: error
              message: auth failed
    """
    try:
        try:
            post_data = await request.json()
        except AttributeError:
            post_data = await request.post()

        # must contain 'username' and 'password'
        if ("username" not in post_data) or (len(post_data["username"]) == 0):
            return web.json_response(
                {"status": "error", "message": "missing username"}, status=400
            )
        if ("password" not in post_data) or (len(post_data["password"]) == 0):
            return web.json_response(
                {"status": "error", "message": "missing password"}, status=400
            )

        # connecting from penquins: check penquins version
        if "penquins.__version__" in post_data:
            penquins_version = post_data["penquins.__version__"]
            if penquins_version not in config["misc"]["supported_penquins_versions"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": "unsupported version of penquins: "
                        f'{post_data["penquins.__version__"]}',
                    },
                    status=400,
                )

        username = str(post_data["username"])
        password = str(post_data["password"])

        try:
            # user exists and passwords match?
            select = await request.app["mongo"].users.find_one({"_id": username})
            if select is not None and check_password_hash(select["password"], password):
                payload = {
                    "user_id": username,
                    "created_at": datetime.datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%S.%f+00:00"
                    ),
                }
                # optionally set expiration date
                if request.app["JWT"]["JWT_EXP_DELTA_SECONDS"] is not None:
                    payload["exp"] = (
                        datetime.datetime.utcnow()
                        + datetime.timedelta(
                            seconds=request.app["JWT"]["JWT_EXP_DELTA_SECONDS"]
                        )
                    ).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
                jwt_token = jwt.encode(
                    payload=payload,
                    key=request.app["JWT"]["JWT_SECRET"],
                    algorithm=request.app["JWT"]["JWT_ALGORITHM"],
                )

                return web.json_response({"status": "success", "token": jwt_token})

            else:
                return web.json_response(
                    {"status": "error", "message": "wrong credentials"}, status=401
                )

        except Exception as _e:
            log(_e)
            _err = traceback.format_exc()
            log(_err)
            return web.json_response(
                {"status": "error", "message": "wrong credentials"}, status=401
            )

    except Exception as _e:
        log(_e)
        _err = traceback.format_exc()
        log(_err)
        return web.json_response(
            {"status": "error", "message": "auth failed"}, status=500
        )


# @routes.get('/', name='ping', allow_head=False)
@auth_required
async def ping(request: web.Request) -> web.Response:
    """
    ping/pong

    :param request:
    :return:

    ---
    summary: ping/pong
    tags:
      - root

    responses:
      '200':
        description: greetings to an authorized user
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                message:
                  type: string
            example:
              status: success
              message: greetings from Kowalski!
    """
    return web.json_response(
        {"status": "success", "message": "greetings from Kowalski!"}, status=200
    )


""" users """


# @routes.post('/api/users')
@admin_required
async def users_post(request: web.Request) -> web.Response:
    """
    Add new user

    :return:

    ---
    summary: Add new user
    tags:
      - users

    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            required:
              - username
              - password
            properties:
              username:
                type: string
              password:
                type: string
              email:
                type: string
              permissions:
                type: string
          example:
            username: noone
            password: nopas!
            email: user@caltech.edu

    responses:
      '200':
        description: added user
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [success]
                message:
                  type: string
            example:
              status: success
              message: added user noone

      '400':
        description: username or password missing in requestBody
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            example:
              status: error
              message: username and password must be set

      '500':
        description: internal/unknown cause of failure
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            example:
              status: error
              message: "failed to add user: <error message>"
    """
    try:
        _data = await request.json()

        username = _data.get("username", "")
        password = _data.get("password", "")
        email = _data.get("email", None)
        permissions = _data.get("permissions", dict())

        if len(username) == 0 or len(password) == 0:
            return web.json_response(
                {"status": "error", "message": "username and password must be set"},
                status=400,
            )

        # add user to coll_usr collection:
        await request.app["mongo"].users.insert_one(
            {
                "_id": username,
                "email": email,
                "password": generate_password_hash(password),
                "permissions": permissions,
                "last_modified": datetime.datetime.now(),
            }
        )

        return web.json_response(
            {"status": "success", "message": f"added user {username}"}, status=200
        )

    except Exception as _e:
        return web.json_response(
            {"status": "error", "message": f"failed to add user: {_e}"}, status=500
        )


# @routes.delete('/api/users/{username}')
@admin_required
async def users_delete(request: web.Request) -> web.Response:
    """
    Remove user

    :return:

    ---
    summary: Remove user
    tags:
      - users

    responses:
      '200':
        description: removed user
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [success]
                message:
                  type: string
            example:
              status: success
              message: removed user noone

      '400':
        description: username not found or is superuser
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            examples:
              attempting superuser removal:
                value:
                  status: error
                  message: cannot remove the superuser!
              username not found:
                value:
                  status: error
                  message: user noone not found

      '500':
        description: internal/unknown cause of failure
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            example:
              status: error
              message: "failed to remove user: <error message>"
    """
    try:
        # get query params
        username = request.match_info["username"]

        if username == config["server"]["admin_username"]:
            return web.json_response(
                {"status": "error", "message": "cannot remove the superuser!"},
                status=400,
            )

        # try to remove the user:
        r = await request.app["mongo"].users.delete_one({"_id": username})

        if r.deleted_count != 0:
            return web.json_response(
                {"status": "success", "message": f"removed user {username}"}, status=200
            )
        else:
            return web.json_response(
                {"status": "error", "message": f"user {username} not found"}, status=400
            )

    except Exception as _e:
        return web.json_response(
            {"status": "error", "message": f"failed to remove user: {_e}"}, status=500
        )


# @routes.put('/api/users/{username}')
@admin_required
async def users_put(request: web.Request) -> web.Response:
    """
    Edit user data

    :return:

    ---
    summary: Edit user data
    tags:
      - users

    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              username:
                type: string
              password:
                type: string
          example:
            username: noone

    responses:
      '200':
        description: edited user
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [success]
                message:
                  type: string
            example:
              status: success
              message: edited user noone

      '400':
        description: cannot rename superuser
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            examples:
              attempting superuser renaming:
                value:
                    status: error
                    message: cannot rename the superuser!

      '500':
        description: internal/unknown cause of failure
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            example:
              status: error
              message: "failed to remove user: <error message>"
    """
    try:
        _data = await request.json()

        _id = request.match_info["username"]
        username = _data.get("username", "")
        password = _data.get("password", "")

        if (
            _id == config["server"]["admin_username"]
            and username != config["server"]["admin_username"]
        ):
            return web.json_response(
                {"status": "error", "message": "cannot rename the superuser!"},
                status=400,
            )

        # change username:
        if (_id != username) and (len(username) > 0):
            select = await request.app["mongo"].users.find_one({"_id": _id})
            select["_id"] = username
            await request.app["mongo"].users.insert_one(select)
            await request.app["mongo"].users.delete_one({"_id": _id})

        # change password:
        if len(password) != 0:
            await request.app["mongo"].users.update_one(
                {"_id": username},
                {
                    "$set": {"password": generate_password_hash(password)},
                    "$currentDate": {"last_modified": True},
                },
            )

        return web.json_response(
            {"status": "success", "message": f"edited user {_id}"}, status=200
        )

    except Exception as _e:
        return web.json_response(
            {"status": "error", "message": f"failed to edit user: {_e}"}, status=500
        )


""" queries """


SYSTEM_COLLECTIONS = ("users", "filters", "queries")
QUERY_TYPES = (
    "cone_search",
    "count_documents",
    "estimated_document_count",
    "find",
    "find_one",
    "aggregate",
    "info",
    "near",
    "drop",
    "skymap",
)
INFO_COMMANDS = (
    "catalog_names",
    "catalog_info",
    "index_info",
    "db_info",
)
FORBIDDEN_STAGES_QUERIES = {"$unionWith", "$out", "$merge"}
FORBIDDEN_STAGES_FILTERS = {"$lookup", "$unionWith", "$out", "$merge"}
ANGULAR_UNITS = ("arcsec", "arcmin", "deg", "rad")


class Query(Model, ABC):
    """Data model for queries for streamlined validation"""

    query_type: str
    query: Optional[Any]
    kwargs: dict = dict()
    user: str

    @staticmethod
    def construct_filter(query: Mapping):
        """Check validity of query filter specs and preprocess if necessary

        :param query: Mapping containing filter specification either as a Mapping or a literal_eval'uable str
        :return:
        """
        catalog_filter = query.get("filter")
        if not isinstance(catalog_filter, (str, Mapping)):
            raise ValueError("Unsupported filter specification")
        if isinstance(catalog_filter, str):
            # passed string? evaluate:
            catalog_filter = literal_eval(catalog_filter.strip())

        return catalog_filter

    @staticmethod
    def construct_projection(query):
        """Check validity of query projection specs and preprocess if necessary

        :param query: Mapping containing projection specification either as a Mapping or a literal_eval'uable str
        :return:
        """
        catalog_projection = query.get("projection")
        if catalog_projection is not None:
            if not isinstance(catalog_projection, (str, Mapping)):
                raise ValueError("Unsupported projection specification")
            if isinstance(catalog_projection, str):
                # passed string? evaluate:
                catalog_projection = literal_eval(catalog_projection.strip())
        else:
            catalog_projection = dict()

        return catalog_projection

    @staticmethod
    def angle_to_rad(angle: Union[float, int], units: str) -> float:
        """Convert angle to rad

        :param angle: angle [deg]
        :param units: str, one of ["arcsec", "arcmin", "deg"]
        :return:
        """
        angle_rad = float(angle)
        if units not in ANGULAR_UNITS:
            raise Exception(f"Angular units not in {ANGULAR_UNITS}")
        if units == "arcsec":
            angle_rad *= np.pi / 180 / 3600
        elif units == "arcmin":
            angle_rad *= np.pi / 180 / 60
        elif units == "deg":
            angle_rad *= np.pi / 180

        return angle_rad

    @staticmethod
    def parse_object_coordinates(coordinates: Union[str, Sequence, Mapping]):
        """
        Parse object coordinates in degrees/HMS_DMS

        :param coordinates: object coordinates in decimal degrees
                            or strings "HH:MM:SS.SSS..." / "DD:MM:SS.SSS..."
                            or strings "HHhMMmSS.SSS...s" / "DDdMMmSS.SSS...s"
            Options:
             - str that is parsed either as ra dec for a single source or stringified Sequence, as below
             - Sequence, such as [(ra1, dec1), (ra2, dec2), ..]
             - Mapping, such as {'object_name': (ra1, dec1), ...}
        :return:
        """
        if isinstance(coordinates, str):
            coordinates = coordinates.strip()

            # comb coordinates for a single source:
            if not coordinates.startswith(("[", "(", "{")):
                a, b = coordinates.split()
                if ("s" in coordinates) or (":" in coordinates):
                    coordinates = f"[('{a}', '{b}')]"
                else:
                    coordinates = f"[({a}, {b})]"

            coordinates = literal_eval(coordinates)

        if isinstance(coordinates, Sequence):
            object_coordinates = coordinates
            # use coords as source ids replacing dots to keep Mongo happy:
            object_names = [
                str(obj_crd).replace(".", "_") for obj_crd in object_coordinates
            ]
        elif isinstance(coordinates, Mapping):
            object_names, object_coordinates = zip(*coordinates.items())
            object_names = list(map(str, object_names))
            object_names = [
                object_name.replace(".", "_") for object_name in object_names
            ]
        else:
            raise ValueError("Unsupported object coordinate specs")

        return object_names, object_coordinates

    @staticmethod
    def validate_kwargs(kwargs: Mapping, known_kwargs: Sequence) -> dict:
        """Allow only known kwargs:
            check that kwargs.keys() are in known_kwargs and ditch those that are not

        :param kwargs:
        :param known_kwargs:
        :return:
        """
        return {
            kk: vv
            for kk, vv in kwargs.items()
            if kk in [*known_kwargs, "max_time_ms", "comment"]
        }

    @root_validator
    def check_query(cls, values):
        """Validate query and preprocess it if necessary"""
        query_type = values.get("query_type")
        query = values.get("query")
        kwargs = values.get("kwargs")
        user = values.get("user")

        if query_type not in QUERY_TYPES:
            raise KeyError(f"query_type {query_type} not in {str(QUERY_TYPES)}")

        # this way, username will be propagated into mongodb's logs
        kwargs["comment"] = user

        if query.get("catalog") is not None:
            catalog = query.get("catalog").strip()
            if catalog in SYSTEM_COLLECTIONS and not is_admin(user):
                raise ValueError("Protected collection")
        if query.get("catalogs") is not None:
            catalogs = query.get("catalogs")
            for catalog in catalogs:
                if catalog in SYSTEM_COLLECTIONS and not is_admin(user):
                    raise ValueError("Protected collection")

        if query_type == "aggregate":
            pipeline = query.get("pipeline")
            if (not isinstance(pipeline, str)) and (not isinstance(pipeline, Sequence)):
                raise ValueError("Unsupported pipeline specification")
            if isinstance(pipeline, str):
                # passed string? evaluate:
                pipeline = literal_eval(pipeline.strip())

            if len(pipeline) == 0:
                raise ValueError("Pipeline must contain at least one stage")

            stages = set([list(stage.keys())[0] for stage in pipeline])
            if len(stages.intersection(FORBIDDEN_STAGES_QUERIES)):
                raise ValueError(
                    f"Pipeline uses forbidden stages: {str(stages.intersection(FORBIDDEN_STAGES_QUERIES))}"
                )

            kwargs = cls.validate_kwargs(
                kwargs=kwargs, known_kwargs=("allowDiskUse", "batchSize")
            )

            values["kwargs"] = kwargs

            values["query"]["pipeline"] = pipeline

        elif query_type == "cone_search":
            # apply filter before positional query?
            filter_first = kwargs.get("filter_first", False)

            # cone search radius:
            cone_search_radius = cls.angle_to_rad(
                angle=query["object_coordinates"]["cone_search_radius"],
                units=query["object_coordinates"]["cone_search_unit"],
            )

            object_names, object_coordinates = cls.parse_object_coordinates(
                query["object_coordinates"]["radec"]
            )

            # reshuffle query to ease execution on Mongo side
            query_preprocessed = dict()
            for catalog in query["catalogs"]:
                catalog = catalog.strip()
                query_preprocessed[catalog] = dict()

                # specifying filter is optional in this case
                if "filter" in query["catalogs"][catalog]:
                    catalog_filter = cls.construct_filter(query["catalogs"][catalog])
                else:
                    catalog_filter = dict()

                # construct projection, which is always optional
                catalog_projection = cls.construct_projection(
                    query["catalogs"][catalog]
                )

                # parse coordinate list
                for oi, obj_crd in enumerate(object_coordinates):
                    # convert ra/dec into GeoJSON-friendly format
                    _ra, _dec = radec_str2geojson(*obj_crd)
                    object_position_query = dict()
                    object_position_query["coordinates.radec_geojson"] = {
                        "$geoWithin": {
                            "$centerSphere": [[_ra, _dec], cone_search_radius]
                        }
                    }
                    # use stringified object coordinates as dict keys and merge dicts with cat/obj queries:
                    if not filter_first:
                        query_preprocessed[catalog][object_names[oi]] = (
                            {**object_position_query, **catalog_filter},
                            {**catalog_projection},
                        )
                    else:
                        # place the filter expression in front of the positional query?
                        # this may be useful if an index exists to speed up the query
                        query_preprocessed[catalog][object_names[oi]] = (
                            {**catalog_filter, **object_position_query},
                            {**catalog_projection},
                        )

            kwargs = cls.validate_kwargs(
                kwargs=kwargs, known_kwargs=("skip", "hint", "limit", "sort")
            )

            values["kwargs"] = kwargs
            values["query"] = query_preprocessed

        elif query_type == "count_documents":
            values["query"]["filter"] = cls.construct_filter(query)

            kwargs = cls.validate_kwargs(
                kwargs=kwargs, known_kwargs=("skip", "hint", "limit")
            )
            values["kwargs"] = kwargs

        elif query_type == "find":
            # construct filter
            values["query"]["filter"] = cls.construct_filter(query)
            # construct projection
            values["query"]["projection"] = cls.construct_projection(query)

            kwargs = cls.validate_kwargs(
                kwargs=kwargs, known_kwargs=("skip", "hint", "limit", "sort")
            )
            values["kwargs"] = kwargs

        elif query_type == "find_one":
            values["query"]["filter"] = cls.construct_filter(query)

            kwargs = cls.validate_kwargs(
                kwargs=kwargs, known_kwargs=("skip", "hint", "limit", "sort")
            )
            values["kwargs"] = kwargs

        elif query_type == "info":
            command = query.get("command")
            if command not in INFO_COMMANDS:
                raise KeyError(f"command {command} not in {str(INFO_COMMANDS)}")

        elif query_type == "near":
            # apply filter before positional query?
            filter_first = kwargs.get("filter_first", False)

            min_distance = cls.angle_to_rad(
                angle=query.get("min_distance", 0),
                units=query.get("distance_units", "rad"),
            )

            max_distance = cls.angle_to_rad(
                angle=query.get("max_distance", np.pi / 180 / 60),  # default to 1'
                units=query.get("distance_units", "rad"),
            )

            object_names, object_coordinates = cls.parse_object_coordinates(
                query["radec"]
            )

            # reshuffle query to ease execution on Mongo side
            query_preprocessed = dict()
            for catalog in query["catalogs"]:
                catalog = catalog.strip()
                query_preprocessed[catalog] = dict()

                # specifying filter is optional in this case
                if "filter" in query["catalogs"][catalog]:
                    catalog_filter = cls.construct_filter(query["catalogs"][catalog])
                else:
                    catalog_filter = dict()

                # construct projection, which is always optional
                catalog_projection = cls.construct_projection(
                    query["catalogs"][catalog]
                )

                # parse coordinate list
                for oi, obj_crd in enumerate(object_coordinates):
                    # convert ra/dec into GeoJSON-friendly format
                    _ra, _dec = radec_str2geojson(*obj_crd)
                    object_position_query = dict()
                    object_position_query["coordinates.radec_geojson"] = {
                        "$nearSphere": [_ra, _dec],
                        "$minDistance": min_distance,
                        "$maxDistance": max_distance,
                    }
                    # use stringified object coordinates as dict keys and merge dicts with cat/obj queries:
                    if not filter_first:
                        query_preprocessed[catalog][object_names[oi]] = (
                            {**object_position_query, **catalog_filter},
                            {**catalog_projection},
                        )
                    else:
                        # place the filter expression in front of the positional query?
                        # this may be useful if an index exists to speed up the query
                        query_preprocessed[catalog][object_names[oi]] = (
                            {**catalog_filter, **object_position_query},
                            {**catalog_projection},
                        )

            kwargs = cls.validate_kwargs(
                kwargs=kwargs, known_kwargs=("skip", "hint", "limit", "sort")
            )

            values["kwargs"] = kwargs
            values["query"] = query_preprocessed

        elif query_type == "skymap":
            # construct filter
            values["query"]["filter"] = cls.construct_filter(query)
            # construct projection
            values["query"]["projection"] = cls.construct_projection(query)

            kwargs = cls.validate_kwargs(
                kwargs=kwargs, known_kwargs=("skip", "hint", "limit", "sort")
            )
            values["kwargs"] = kwargs
            # apply filter before positional query?
            filter_first = kwargs.get("filter_first", False)

            dateobs = query.get("skymap", {}).get("dateobs")
            trigger_id = query.get("skymap", {}).get("trigger_id")
            localization_name = query.get("skymap", {}).get("localization_name")
            aliases = query.get("skymap", {}).get("aliases", [])

            contour = query.get("skymap", {}).get("contour", 90)

            if not localization_name:
                raise ValueError("name must be specified")

            if not any([dateobs, trigger_id, aliases]):
                raise ValueError(
                    "At least one of dateobs, trigger_id, skymap_name, aliases must be specified"
                )

            # we write the skymap query
            skymap_query = {
                "$or": [],
                "localization_name": localization_name,
            }
            if dateobs:
                if isinstance(dateobs, str):
                    try:
                        dateobs = datetime.datetime.strptime(
                            dateobs, "%Y-%m-%dT%H:%M:%S.%f"
                        )
                    except ValueError:
                        try:
                            dateobs = datetime.datetime.strptime(
                                dateobs, "%Y-%m-%dT%H:%M:%S"
                            )
                        except ValueError:
                            raise ValueError(
                                "dateobs must be in format YYYY-MM-DDTHH:MM:SS[.SSS]"
                            )

                skymap_query["$or"].append({"dateobs": dateobs})
            if trigger_id:
                skymap_query["$or"].append({"trigger_id": trigger_id})
            if aliases:
                skymap_query["$or"].append({"aliases": {"$in": aliases}})

            skymap_query = (
                skymap_query,
                {"contours": 1},
            )

            values["query"]["skymap_query"] = skymap_query
            values["query"]["contour_level"] = contour

        return values


class QueryHandler(Handler):
    """Handlers to work with user queries"""

    @auth_required
    async def post(self, request: web.Request) -> web.Response:
        """Query Kowalski

        ---
        summary: Query Kowalski
        tags:
          - queries

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
                required:
                  - query_type
                  - query
                properties:
                  query_type:
                    type: string
                    enum:
                      - aggregate
                      - cone_search
                      - count_documents
                      - estimated_document_count
                      - find
                      - find_one
                      - info
                      - near
                  query:
                    type: object
                    description: query. depends on query_type, see examples
                    oneOf:
                      - $ref: "#/components/schemas/aggregate"
                      - $ref: "#/components/schemas/cone_search"
                      - $ref: "#/components/schemas/count_documents"
                      - $ref: "#/components/schemas/estimated_document_count"
                      - $ref: "#/components/schemas/find"
                      - $ref: "#/components/schemas/find_one"
                      - $ref: "#/components/schemas/info"
                      - $ref: "#/components/schemas/near"
                  kwargs:
                    type: object
                    description: additional parameters. depends on query_type, see examples
                    oneOf:
                      - $ref: "#/components/schemas/aggregate_kwargs"
                      - $ref: "#/components/schemas/cone_search_kwargs"
                      - $ref: "#/components/schemas/count_documents_kwargs"
                      - $ref: "#/components/schemas/estimated_document_count_kwargs"
                      - $ref: "#/components/schemas/find_kwargs"
                      - $ref: "#/components/schemas/find_one_kwargs"
                      - $ref: "#/components/schemas/info_kwargs"
                      - $ref: "#/components/schemas/near_kwargs"
              examples:
                aggregate:
                  value:
                    "query_type": "aggregate"
                    "query": {
                      "catalog": "ZTF_alerts",
                      "pipeline": [
                        {'$match': {'candid': 1105522281015015000}},
                        {"$project": {"_id": 0, "candid": 1, "candidate.drb": 1}}
                      ],
                    }
                    "kwargs": {
                      "max_time_ms": 2000
                    }

                cone_search:
                  value:
                    "query_type": "cone_search"
                    "query": {
                      "object_coordinates": {
                        "cone_search_radius": 2,
                        "cone_search_unit": "arcsec",
                        "radec": {"object1": [71.6577756, -10.2263957]}
                      },
                      "catalogs": {
                        "ZTF_alerts": {
                          "filter": {},
                          "projection": {"_id": 0, "candid": 1, "objectId": 1}
                        }
                      }
                    }
                    "kwargs": {
                      "filter_first": False
                    }

                find:
                  value:
                    "query_type": "find"
                    "query": {
                      "catalog": "ZTF_alerts",
                      "filter": {'candidate.drb': {"$gt": 0.9}},
                      "projection": {"_id": 0, "candid": 1, "candidate.drb": 1},
                    }
                    "kwargs": {
                      "sort": [["$natural", -1]],
                      "limit": 2
                    }

                find_one:
                  value:
                    "query_type": "find_one"
                    "query": {
                      "catalog": "ZTF_alerts",
                      "filter": {}
                    }

                info:
                  value:
                    "query_type": "info"
                    "query": {
                      "command": "catalog_names"
                    }

                count_documents:
                  value:
                    "query_type": "count_documents"
                    "query": {
                      "catalog": "ZTF_alerts",
                      "filter": {"objectId": "ZTF20aakyoez"}
                    }

                drop:
                  value:
                    "query_type": "drop"
                    "query": {
                      "catalog": "ZTF_alerts",
                    }

                estimated_document_count:
                  value:
                    "query_type": "estimated_document_count"
                    "query": {
                      "catalog": "ZTF_alerts"
                    }

                near:
                  value:
                    "query_type": "near"
                    "query": {
                      "max_distance": 30,
                      "distance_units": "arcsec",
                      "radec": {"object1": [71.6577756, -10.2263957]},
                      "catalogs": {
                        "ZTF_alerts": {
                          "filter": {},
                          "projection": {"_id": 0, "candid": 1, "objectId": 1}
                        }
                      }
                    }
                    "kwargs": {
                      "limit": 1
                    }

        responses:
          '200':
            description: query result
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - user
                    - message
                    - status
                  properties:
                    status:
                      type: string
                      enum: [success]
                    message:
                      type: string
                    user:
                      type: string
                    kwargs:
                      type: object
                    data:
                      oneOf:
                        - type: number
                        - type: array
                        - type: object
                examples:
                  aggregate:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": [
                        {
                          "candid": 1105522281015015000,
                          "candidate": {
                            "drb": 0.999999463558197
                          }
                        }
                      ]

                  cone_search:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": {
                        "ZTF_alerts": {
                          "object1": [
                            {"objectId": "ZTF20aaelulu",
                             "candid": 1105522281015015000}
                          ]
                        }
                      }

                  find:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": [
                        {
                          "candid": 1127561444715015009,
                          "candidate": {
                            "drb": 0.9999618530273438
                          }
                        },
                        {
                          "candid": 1127107111615015007,
                          "candidate": {
                            "drb": 0.9986417293548584
                          }
                        }
                      ]

                  info:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": [
                        "ZTF_alerts_aux",
                        "ZTF_alerts"
                      ]

                  count_documents:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": 1

                  drop:
                    value:
                      "status": "success"
                      "message": "Successfully dropped collection"
                      "data": 1

                  estimated_document_count:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": 11

                  near:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": {
                        "ZTF_alerts": {
                          "object1": [
                            {"objectId": "ZTF20aaelulu",
                             "candid": 1105522281015015000}
                          ]
                        }
                      }

          '400':
            description: query parsing/execution error
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [error]
                    message:
                      type: string
                examples:
                  unknown query type:
                    value:
                      status: error
                      message: "query_type not in ('cone_search', 'count_documents', 'estimated_document_count', 'find', 'find_one', 'aggregate', 'info')"
                  random error:
                    value:
                      status: error
                      message: "failure: <error message>"
        """
        # allow both .json() and .post():
        try:
            query_spec = await request.json()
        except AttributeError:
            query_spec = await request.post()

        # this is set by auth_middleware
        query_spec["user"] = request.user

        # validate and preprocess
        query = Query(**query_spec)

        # by default, long-running queries will be killed after config['misc']['max_time_ms'] ms
        max_time_ms = int(
            query.kwargs.get("max_time_ms", config["misc"]["max_time_ms"])
        )
        if max_time_ms < 1:
            raise ValueError("max_time_ms must be int >= 1")
        query.kwargs.pop("max_time_ms", None)

        # execute query, depending on query.query_type
        data = dict()

        if query.query_type == "drop":
            catalog = query.query["catalog"]
            cursor = request.app["mongo"][catalog].drop()
            data = await cursor
            return self.success(message="Successfully dropped collection", data=data)

        if query.query_type in ("cone_search", "near"):
            # iterate over catalogs
            for catalog in query.query:
                data[catalog] = dict()
                # iterate over objects:
                for obj in query.query[catalog]:
                    # project?
                    if len(query.query[catalog][obj][1]) > 0:
                        cursor = request.app["mongo"][catalog].find(
                            query.query[catalog][obj][0],
                            query.query[catalog][obj][1],
                            max_time_ms=max_time_ms,
                            **query.kwargs,
                        )
                    # return the whole documents by default
                    else:
                        cursor = request.app["mongo"][catalog].find(
                            query.query[catalog][obj][0],
                            max_time_ms=max_time_ms,
                            **query.kwargs,
                        )
                    data[catalog][obj] = await cursor.to_list(length=None)

        if query.query_type == "find":
            catalog = query.query["catalog"]
            catalog_filter = query.query["filter"]
            catalog_projection = query.query["projection"]
            # project?
            if len(catalog_projection) > 0:
                cursor = request.app["mongo"][catalog].find(
                    catalog_filter,
                    catalog_projection,
                    max_time_ms=max_time_ms,
                    **query.kwargs,
                )
            # return the whole documents by default
            else:
                cursor = request.app["mongo"][catalog].find(
                    catalog_filter,
                    max_time_ms=max_time_ms,
                    **query.kwargs,
                )

            if isinstance(cursor, (int, float, Sequence, Mapping)) or (cursor is None):
                data = cursor
            else:
                data = await cursor.to_list(length=None)

        if query.query_type == "find_one":
            catalog = query.query["catalog"]
            cursor = request.app["mongo"][catalog].find_one(
                query.query["filter"],
                max_time_ms=max_time_ms,
            )

            data = await cursor

        if query.query_type == "count_documents":
            catalog = query.query["catalog"]
            cursor = request.app["mongo"][catalog].count_documents(
                query.query["filter"],
                maxTimeMS=max_time_ms,
            )
            data = await cursor

        if query.query_type == "estimated_document_count":
            catalog = query.query["catalog"]
            cursor = request.app["mongo"][catalog].estimated_document_count(
                maxTimeMS=max_time_ms,
            )
            data = await cursor

        if query.query_type == "aggregate":
            catalog = query.query["catalog"]
            pipeline = query.query["pipeline"]
            cursor = request.app["mongo"][catalog].aggregate(
                pipeline,
                allowDiskUse=query.kwargs.get("allowDiskUse", True),
                maxTimeMS=max_time_ms,
            )

            data = await cursor.to_list(length=None)

        if query.query_type == "info":
            if query.query["command"] == "catalog_names":
                # get available catalog names
                catalog_names = await request.app["mongo"].list_collection_names()
                data = [
                    catalog_name
                    for catalog_name in sorted(catalog_names)[::-1]
                    if catalog_name not in SYSTEM_COLLECTIONS
                ]
            elif query.query["command"] == "catalog_info":
                catalog = query.query["catalog"]
                data = await request.app["mongo"].command("collstats", catalog)
            elif query.query["command"] == "index_info":
                catalog = query.query["catalog"]
                data = await request.app["mongo"][catalog].index_information()
            elif query.query["command"] == "db_info":
                data = await request.app["mongo"].command("dbstats")

        if query.query_type == "skymap":
            catalog = query.query["catalog"]
            catalog_filter = query.query["filter"]
            catalog_projection = query.query["projection"]

            skymap_query = query.query["skymap_query"]
            contour_level = query.query["contour_level"]
            skymap = await request.app["mongo"][
                config["database"]["collections"]["skymaps"]
            ].find_one(
                skymap_query[0],
                skymap_query[1],
                max_time_ms=max_time_ms,
                **query.kwargs,
            )
            if skymap is None:
                raise ValueError("Skymap not found")
            if f"contour{contour_level}" not in skymap["contours"]:
                raise ValueError(f"Contour level {contour_level} not found in skymap")

            catalog_filter = {
                "$and": [
                    catalog_filter,
                    {
                        "coordinates.radec_geojson": {
                            "$geoWithin": {
                                "$geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": skymap["contours"][
                                        f"contour{contour_level}"
                                    ]["coordinates"],
                                }
                            }
                        }
                    },
                ]
            }

            # project?
            if len(catalog_projection) > 0:
                cursor = request.app["mongo"][catalog].find(
                    catalog_filter,
                    catalog_projection,
                    max_time_ms=max_time_ms,
                    **query.kwargs,
                )
            # return the whole documents by default
            else:
                cursor = request.app["mongo"][catalog].find(
                    catalog_filter,
                    max_time_ms=max_time_ms,
                    **query.kwargs,
                )

            if isinstance(cursor, (int, float, Sequence, Mapping)) or (cursor is None):
                data = cursor
            else:
                data = await cursor.to_list(length=None)

        return self.success(message="Successfully executed query", data=data)


""" filters """


class FilterVersion(EmbeddedModel, ABC):
    """Data model for Filter versions"""

    fid: str = Field(default_factory=uid)
    pipeline: str
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    @root_validator
    def check_min_stages(cls, values):
        pipeline = values.get("pipeline")
        if len(loads(pipeline)) == 0:  # it is stored as a string
            raise ValueError("pipeline must contain at least one stage")
        return values

    @root_validator
    def check_forbidden_stages(cls, values):
        pipeline = values.get("pipeline")
        # check that only allowed stages are used in the pipeline
        stages = set([list(stage.keys())[0] for stage in loads(pipeline)])
        if len(stages.intersection(FORBIDDEN_STAGES_FILTERS)):
            raise ValueError(
                f"pipeline uses forbidden stages: {str(stages.intersection(FORBIDDEN_STAGES_FILTERS))}"
            )
        return values

    class Config:
        json_dumps = dumps
        json_loads = loads
        parse_doc_with_default_factories = True


class Filter(Model, ABC):
    """Data model for Filters"""

    filter_id: int = Field(ge=1)
    group_id: int = Field(ge=1)
    catalog: str
    permissions: List[int] = list()
    active: bool = True
    autosave: Union[bool, dict] = False
    auto_followup: dict = dict()
    update_annotations: bool = False
    active_fid: Optional[str] = Field(min_length=6, max_length=6)
    fv: List[FilterVersion] = list()
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    last_modified: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    class Config:
        # collection name in MongoDB
        collection = "filters"
        json_dumps = dumps
        json_loads = loads
        parse_doc_with_default_factories = True


class FilterHandler(Handler):
    """Handlers to work with user-defined alert filters"""

    @admin_required
    async def get(self, request: web.Request) -> web.Response:
        """Retrieve filter by filter_id

        :param request:
        :return:
        ---
        summary: Retrieve user-defined filters
        tags:
          - filters

        parameters:
          - in: query
            name: filter_id
            description: filter id
            required: true
            schema:
              type: integer
              minimum: 1

        responses:
          '200':
            description: retrieved filter data
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                    - data
                  properties:
                    status:
                      type: string
                      enum: [success]
                    message:
                      type: string
                    data:
                      type: object
                example:
                  "status": "success"
                  "message": "Retrieved filter id 1"
                  "data": {
                    "group_id": 1,
                    "filter_id": 1,
                    "catalog": "ZTF_alerts",
                    "permissions": [1, 2],
                    "autosave": false,
                    "auto_followup": {
                      "active": true,
                      "comment": "SEDM triggered by BTSbot",
                      "payload": {
                        "observation_type": "IFU"
                      },
                      "pipeline": [
                        {
                          "$match": {
                            "candidate.drb": {
                              "$gt": 0.9999
                            }
                          }
                        }
                      ]
                    },
                    "update_annotations": false,
                    "active": true,
                    "active_fid": "nnsun9",
                    "fv": [
                      "fid": "nnsun9",
                      "pipeline": "<serialized extended json string>",
                      "created": {
                          "$date": 1584403506877
                      }
                    ]
                  }

          '400':
            description: retrieval failed or internal/unknown cause of failure
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [error]
                    message:
                      type: string
                example:
                  status: error
                  message: "failure: <error message>"
        """
        filter_id = int(request.match_info["filter_id"])

        filtr = await request.app["mongo_odm"].find_one(
            Filter, Filter.filter_id == filter_id
        )

        if filtr is not None:
            return self.success(
                message=f"Retrieved filter id {filter_id}", data=filtr.doc()
            )
        return self.error(message=f"Filter id {filter_id} not found")

    @admin_required
    async def post(self, request: web.Request) -> web.Response:
        """Post user-defined alert filter, or a new version thereof
        - store pipeline as serialized extended json string,
          to be used with literal_eval to convert to dict at execution
        - run a simple sanity check before saving

        ---
        summary: Post user-defined alert filter, or a new version thereof
        tags:
          - filters

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
                required:
                  - group_id
                  - filter_id
                  - catalog
                  - permissions
                  - pipeline
                properties:
                  group_id:
                    type: integer
                    description: "[fritz] user group (science program) id"
                    minimum: 1
                  filter_id:
                    type: integer
                    description: "[fritz] science program filter id for this user group id"
                    minimum: 1
                  catalog:
                    type: string
                    description: "alert stream to filter"
                    enum: [ZTF_alerts, ZUDS_alerts]
                  permissions:
                    type: array
                    items:
                      type: integer
                    description: "permissions to access streams"
                    minItems: 1
                  autosave:
                    type: boolean
                    description: "automatically save passing alerts to group <group_id>. Optionally, specify an additional filtering layer"
                    default: false
                  auto_followup:
                    type: object
                    description: "automatically trigger follow-up observations for passing alerts, with an additional filtering layer"
                  update_annotations:
                    type: boolean
                    description: "update existing annotations for newly passing alerts"
                    default: false
                  pipeline:
                    type: array
                    items:
                      type: object
                    description: "user-defined aggregation pipeline stages in MQL"
                    minItems: 1
              examples:
                filter_1:
                  value:
                    "group_id": 1
                    "filter_id": 1
                    "catalog": ZTF_alerts
                    "permissions": [1, 2]
                    "pipeline": [
                    {
                      "$match": {
                        "candidate.drb": {
                          "$gt": 0.9999
                        },
                        "cross_matches.CLU_20190625.0": {
                          "$exists": False
                        }
                      }
                    },
                    {
                      "$addFields": {
                        "annotations.author": "dd",
                        "annotations.mean_rb": {"$avg": "$prv_candidates.rb"}
                      }
                    },
                    {
                      "$project": {
                        "_id": 0,
                        "candid": 1,
                        "objectId": 1,
                        "annotations": 1
                      }
                    }
                    ]
                filter_2:
                  value:
                    "group_id": 2
                    "filter_id": 1
                    "catalog": ZTF_alerts
                    "permissions": [1, 2, 3]
                    "autosave": true
                    "update_annotations": false
                    "pipeline": [
                    {
                      "$match": {
                        "candidate.drb": {
                          "$gt": 0.9999
                        },
                        "cross_matches.CLU_20190625.0": {
                          "$exists": True
                        }
                      }
                    },
                    {
                      "$addFields": {
                        "annotations.author": "dd",
                        "annotations.mean_rb": {"$avg": "$prv_candidates.rb"}
                      }
                    },
                    {
                      "$project": {
                        "_id": 0,
                        "candid": 1,
                        "objectId": 1,
                        "annotations": 1
                      }
                    }
                    ]
                filter_3:
                  value:
                    "group_id": 2
                    "filter_id": 1
                    "catalog": ZTF_alerts
                    "permissions": [1, 2, 3]
                    "autosave": {
                      "active": true,
                      "pipeline": [
                        {
                          "$match": {
                            "candidate.drb": {
                              "$gt": 0.9999
                            },
                            "cross_matches.CLU_20190625.0": {
                              "$exists": True
                            }
                          }
                        }
                      ]
                    }
                    "auto_followup": {
                      "allocation_id": 1,
                      "target_group_ids": [1, 2],
                      "comment": "SEDM triggered by BTSbot",
                      "payload": {
                        "observation_type": "IFU",
                      },
                      "active": true,
                      "pipeline": [
                        {
                          "$match": {
                            "candidate.drb": {
                              "$gt": 0.9999
                            },
                            "cross_matches.CLU_20190625.0": {
                              "$exists": True
                            },
                            "classifications.acai_n": {
                                "$gt": 0.8
                            }
                          }
                        }
                      ],
                    }
                    "update_annotations": false
                    "pipeline": [
                    {
                      "$match": {
                        "candidate.drb": {
                          "$gt": 0.9999
                        },
                      }
                    },
                    {
                      "$addFields": {
                        "annotations.author": "dd",
                        "annotations.mean_rb": {"$avg": "$prv_candidates.rb"}
                      }
                    },
                    {
                      "$project": {
                        "_id": 0,
                        "candid": 1,
                        "objectId": 1,
                        "annotations": 1
                      }
                    }
                    ]


        responses:
          '200':
            description: filter successfully saved
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                    - data
                  properties:
                    status:
                      type: string
                      enum: [success]
                    message:
                      type: string
                    user:
                      type: string
                    data:
                      description: "contains unique filter identifier"
                      type: object
                      additionalProperties:
                        type: object
                        properties:
                          fid:
                            type: string
                            description: "generated unique filter identifier"
                            minLength: 6
                            maxLength: 6
                example:
                  "status": "success"
                  "message": "saved filter: c3ig1t"
                  "data": {
                   "fid": "c3ig1t"
                  }

          '400':
            description: filter parsing/testing/saving error
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [error]
                    message:
                      type: string
                example:
                  status: error
                  message: "failure: <error message>"
        """
        # allow both .json() and .post():
        try:
            filter_spec = await request.json()
        except AttributeError:
            filter_spec = await request.post()

        filter_new = Filter(**filter_spec)

        # a user is not allowed to setup auto_followup if autosave is not set
        if filter_new.auto_followup and (
            filter_new.autosave is False
            or (
                isinstance(filter_new.autosave, dict)
                and filter_new.autosave["active"] is False
            )
        ):
            return self.error(
                message="Cannot setup auto_followup without autosave enabled"
            )

        # check if a filter for these (group_id, filter_id) already exists:
        filter_existing = await request.app["mongo_odm"].find_one(
            Filter,
            Filter.filter_id == filter_new.filter_id,
            Filter.group_id == filter_new.group_id,
        )

        # new filter version:
        pipeline = filter_spec.get("pipeline")
        if not isinstance(pipeline, str):
            pipeline = dumps(pipeline)
        filter_version = FilterVersion(pipeline=pipeline)
        try:
            # try on most recently ingested alert to check correctness
            n_docs = await request.app["mongo"][
                filter_new.catalog
            ].estimated_document_count()
            log(f"Found {n_docs} documents in {filter_new.catalog} collection")

            if n_docs > 0:
                # get latest candid:
                select = (
                    request.app["mongo"][filter_new.catalog]
                    .find({}, {"_id": 0, "candid": 1})
                    .sort([("$natural", -1)])
                    .limit(1)
                )
                alert = await select.to_list(length=1)
                alert = alert[0]

                # filter pipeline upstream: select current alert, ditch cutouts, and merge with aux data
                # including archival photometry and cross-matches:
                filter_pipeline_upstream = config["database"]["filters"][
                    filter_new.catalog
                ]
                filter_template = filter_pipeline_upstream + loads(
                    filter_version.pipeline
                )
                # match candid
                filter_template[0]["$match"]["candid"] = alert["candid"]
                # match permissions for ZTF
                if filter_new.catalog.startswith("ZTF"):
                    filter_template[0]["$match"]["candidate.programid"][
                        "$in"
                    ] = filter_new.permissions
                    filter_template[3]["$project"]["prv_candidates"]["$filter"]["cond"][
                        "$and"
                    ][0]["$in"][1] = filter_new.permissions
                    if "fp_hists" in filter_template[3]["$project"]:
                        filter_template[3]["$project"]["fp_hists"]["$filter"]["cond"][
                            "$and"
                        ][0]["$in"][1] = filter_new.permissions
                cursor = request.app["mongo"][filter_new.catalog].aggregate(
                    filter_template, allowDiskUse=False, maxTimeMS=3000
                )
                await cursor.to_list(length=None)
                test_successful, test_message = (
                    True,
                    f"pipeline test for filter id {filter_new.filter_id} successful",
                )
                log(test_message)
            else:
                test_successful, test_message = (
                    True,
                    f"WARNING: No documents in {filter_new.catalog} collection, "
                    f"cannot properly test pipeline for filter id {filter_new.filter_id}",
                )
                log(test_message)
        except Exception as e:
            log(e)
            test_successful, test_message = False, str(e)
        if not test_successful:
            return self.error(message=test_message)

        for attribute in ["autosave", "auto_followup"]:
            if not isinstance(getattr(filter_new, attribute), dict):
                continue
            if attribute == "autosave":
                # verify that the keys are a subset of the allowed keys
                if not set(getattr(filter_new, attribute).keys()).issubset(
                    set(AUTOSAVE_KEYS)
                ):
                    return self.error(
                        message=f"Cannot test {attribute} pipeline: invalid keys"
                    )
            if attribute == "auto_followup":
                # verify that the keys are a subset of the allowed keys
                if not set(getattr(filter_new, attribute).keys()).issubset(
                    set(AUTO_FOLLOWUP_KEYS)
                ):
                    return self.error(
                        message=f"Cannot test {attribute} pipeline: invalid keys"
                    )
            if attribute == "autosave" and "pipeline" not in getattr(
                filter_new, attribute
            ):
                continue
            elif attribute == "auto_followup" and getattr(filter_new, attribute) == {}:
                continue
            elif attribute == "auto_followup" and "pipeline" not in getattr(
                filter_new, attribute
            ):
                return self.error(
                    message=f"Cannot test {attribute} pipeline: no pipeline specified"
                )
            elif attribute == "auto_followup" and "priority_order" in getattr(
                filter_new, attribute
            ):
                if getattr(filter_new, attribute)["priority_order"] not in [
                    "asc",
                    "desc",
                ]:
                    return self.error(
                        message=f"Invalid priority_order specified for auto_followup: {getattr(filter_new, attribute)['priority_order']}"
                    )
                new_priority_order = getattr(filter_new, attribute)["priority_order"]
                new_allocation_id = getattr(filter_new, attribute)["allocation_id"]
                # fetch all existing filters in the DB with auto_followup with the same allocation_id
                filters_same_alloc = [
                    f
                    async for f in request.app["mongo"]["filters"].find(
                        {"auto_followup.allocation_id": int(new_allocation_id)}
                    )
                ]
                for f in filters_same_alloc:
                    if (
                        "priority_order" in f["auto_followup"]
                        and f["auto_followup"]["priority_order"] != new_priority_order
                    ):
                        return self.error(
                            message="Cannot add new filter with auto_followup: existing filters with the same allocation have priority_order set to a different value, which is unexpected"
                        )
            pipeline = getattr(filter_new, attribute).get("pipeline")
            if not isinstance(pipeline, str):
                pipeline = dumps(pipeline)
            catalog = (
                filter_existing.catalog
                if filter_existing is not None
                else filter_new.catalog
            )
            permissions = (
                filter_existing.permissions
                if filter_existing is not None
                else filter_new.permissions
            )
            n_docs = await request.app["mongo"][catalog].estimated_document_count()
            if n_docs > 0:
                # get latest candid:
                select = (
                    request.app["mongo"][catalog]
                    .find({}, {"_id": 0, "candid": 1})
                    .sort([("$natural", -1)])
                    .limit(1)
                )
                alert = await select.to_list(length=1)
                alert = alert[0]

                # filter pipeline upstream: select current alert, ditch cutouts, and merge with aux data
                # including archival photometry and cross-matches:
                filter_pipeline_upstream = config["database"]["filters"][catalog]
                filter_template = filter_pipeline_upstream + loads(pipeline)
                # match candid
                filter_template[0]["$match"]["candid"] = alert["candid"]
                # match permissions for ZTF
                if catalog.startswith("ZTF"):
                    filter_template[0]["$match"]["candidate.programid"][
                        "$in"
                    ] = permissions
                    filter_template[3]["$project"]["prv_candidates"]["$filter"]["cond"][
                        "$and"
                    ][0]["$in"][1] = permissions
                    if "fp_hists" in filter_template[3]["$project"]:
                        filter_template[3]["$project"]["fp_hists"]["$filter"]["cond"][
                            "$and"
                        ][0]["$in"][1] = permissions
                try:
                    cursor = request.app["mongo"][catalog].aggregate(
                        filter_template, allowDiskUse=False, maxTimeMS=3000
                    )
                    await cursor.to_list(length=None)
                    setattr(
                        filter_new,
                        attribute,
                        {**getattr(filter_new, attribute), "pipeline": pipeline},
                    )
                except Exception as e:
                    return self.error(
                        message=f"Cannot test {attribute} pipeline: {str(e)}"
                    )
            else:
                return self.error(
                    message=f"Cannot test {attribute} pipeline: no documents in {catalog} collection"
                )

        # if a filter does not exist for (filter_id, group_id), create one:
        if filter_existing is None:
            filter_new.fv.append(filter_version)
            filter_new.active_fid = filter_version.fid
            filter_new.last_modified = datetime.datetime.now()
            await request.app["mongo_odm"].save(filter_new)
        else:
            # already exists? push new filter version and reset active_fid:
            filter_existing.fv.append(filter_version)
            filter_existing.active_fid = filter_version.fid
            filter_existing.last_modified = datetime.datetime.now()
            # note: filters are defined on streams on SkyPortal,
            # with non-modifiable catalog and permissions parameters, so it should not be possible to modify such here
            await request.app["mongo_odm"].save(filter_existing)

        return self.success(
            message=test_message + f"\nsaved new filter version: {filter_version.fid}",
            data=filter_version.doc(),
        )

    @admin_required
    async def patch(self, request: web.Request) -> web.Response:
        """Update user-defined filter

        :param request:
        :return:

        ---
        summary: "Modify existing filters: activate/deactivate, set active_fid, autosave, auto_followup, or update_annotations"
        tags:
          - filters

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
                required:
                  - filter_id
                properties:
                  filter_id:
                    type: integer
                    description: "[fritz] filter id for this group id"
                    minimum: 1
                  active:
                    type: boolean
                    description: "activate or deactivate filter"
                  active_fid:
                    description: "set fid as active version"
                    type: string
                    minLength: 6
                    maxLength: 6
                  autosave:
                    type: boolean
                    description: "autosave candidates that pass filter to corresponding group, which an optional additional filtering layer"
                  auto_followup:
                    type: object
                    description: "automatically trigger follow-up observations for passing alerts, with an additional filtering layer"
                  update_annotations:
                    type: boolean
                    description: "update annotations for new candidates that previously passed filter?"

              examples:
                filter_1:
                  value:
                    "filter_id": 1
                    "active": false
                filter_2:
                  value:
                    "filter_id": 5
                    "active_fid": "r7qiti"
                filter_3:
                  value:
                    "filter_id": 1
                    "autosave": true
                filter_3:
                  value:
                    "filter_id": 1
                    "autosave": {
                      "active": true,
                      "comment": "Saved automatically by Kowalski bot",
                      "pipeline": [
                        {
                          "$match": {
                            "candidate.drb": {
                              "$gt": 0.9999
                            }
                          }
                        }
                      ]
                    }
                filter_4:
                  value:
                    "filter_id": 1
                    "auto_followup": {
                      "active": true,
                      "allocation_id": 1,
                      "comment": "SEDM triggered by BTSbot",
                      "payload": {
                        "observation_type": "IFU"
                      },
                      "pipeline": [
                        {
                          "$match": {
                            "candidate.drb": {
                              "$gt": 0.9999
                            }
                          }
                        }
                      ]
                    }
                filter_5:
                  value:
                    "filter_id": 1
                    "update_annotations": true

        responses:
          '200':
            description: filter updated
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [success]
                    message:
                      type: string
                example:
                  status: success
                  message: "updated filter id 1"
                  data:
                    active: false

          '400':
            description: filter not found or removal failed
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [error]
                    message:
                      type: string
                examples:
                  filter not found:
                    value:
                      status: error
                      message: Filter id 1 not found
        """
        # allow both .json() and .post():
        try:
            filter_spec = await request.json()
        except AttributeError:
            filter_spec = await request.post()

        filter_id = filter_spec.get("filter_id")

        # check if a filter for these (group_id, filter_id) already exists:
        filter_existing = await request.app["mongo_odm"].find_one(
            Filter, Filter.filter_id == filter_id
        )
        if filter_existing is None:
            return self.error(message=f"Filter id {filter_id} not found")

        filter_doc = filter_existing.doc()

        # note: partial model loading is not (yet?) available in odmantic + need a custom check on active_fid
        for modifiable_field in (
            "active",
            "active_fid",
            "autosave",
            "auto_followup",
            "update_annotations",
        ):
            value = filter_spec.get(modifiable_field)
            if value is not None:
                if modifiable_field == "active_fid" and value not in [
                    filter_version["fid"] for filter_version in filter_doc["fv"]
                ]:
                    raise ValueError(
                        f"Cannot set active_fid to {value}: filter version fid not in filter.fv"
                    )
                elif modifiable_field in ["autosave", "auto_followup"]:
                    # verify that the keys of autosave are in the AUTOSAVE_KEYS set
                    if (
                        modifiable_field == "autosave"
                        and isinstance(value, dict)
                        and not set(value.keys()).issubset(set(AUTOSAVE_KEYS))
                    ):
                        return self.error(
                            message=f"Cannot update filter id {filter_id}: {modifiable_field} contains invalid keys"
                        )
                    # verify that the keys of auto_followup are in the AUTO_FOLLOWUP_KEYS set
                    elif (
                        modifiable_field == "auto_followup"
                        and isinstance(value, dict)
                        and not set(value.keys()).issubset(set(AUTO_FOLLOWUP_KEYS))
                    ):
                        return self.error(
                            message=f"Cannot update filter id {filter_id}: {modifiable_field} contains invalid keys"
                        )
                    elif (
                        modifiable_field == "auto_followup"
                        and isinstance(value, dict)
                        and "priority_order" in value
                    ):
                        # fetch all existing filters in the DB with auto_followup with the same allocation_id
                        filters_same_alloc = [
                            f
                            async for f in request.app["mongo"]["filters"].find(
                                {
                                    "auto_followup.allocation_id": int(
                                        value["allocation_id"]
                                    )
                                }
                            )
                        ]
                        # if there is any filter with the same allocation_id, and a non null priority_order that is differet
                        # throw an error. This is to avoid having multiple filters with the same allocation_id and different
                        # priority_order, which should be fixed by a sys admin, as it should not happen
                        for f in filters_same_alloc:
                            if (
                                "priority_order" in f["auto_followup"]
                                and f["auto_followup"]["priority_order"]
                                != value["priority_order"]
                            ):
                                return self.error(
                                    message=f"Cannot update filter id {filter_id}: {modifiable_field}, filters with the same allocation have priority_order set to a different value, which is unexpected"
                                )
                    if modifiable_field == "autosave" and isinstance(value, bool):
                        pass
                    elif isinstance(value, dict) and "pipeline" not in value:
                        pass
                    else:
                        pipeline = value.get("pipeline")
                        if not isinstance(pipeline, str):
                            pipeline = dumps(pipeline)
                        n_docs = await request.app["mongo"][
                            filter_existing.catalog
                        ].estimated_document_count()
                        if n_docs > 0:
                            # get latest candid:
                            select = (
                                request.app["mongo"][filter_existing.catalog]
                                .find({}, {"_id": 0, "candid": 1})
                                .sort([("$natural", -1)])
                                .limit(1)
                            )
                            alert = await select.to_list(length=1)
                            alert = alert[0]

                            # filter pipeline upstream: select current alert, ditch cutouts, and merge with aux data
                            # including archival photometry and cross-matches:
                            filter_pipeline_upstream = config["database"]["filters"][
                                filter_existing.catalog
                            ]
                            filter_template = filter_pipeline_upstream + loads(pipeline)
                            # match candid
                            filter_template[0]["$match"]["candid"] = alert["candid"]
                            # match permissions for ZTF
                            if filter_existing.catalog.startswith("ZTF"):
                                filter_template[0]["$match"]["candidate.programid"][
                                    "$in"
                                ] = filter_existing.permissions
                                filter_template[3]["$project"]["prv_candidates"][
                                    "$filter"
                                ]["cond"]["$and"][0]["$in"][
                                    1
                                ] = filter_existing.permissions
                                if "fp_hists" in filter_template[3]["$project"]:
                                    filter_template[3]["$project"]["fp_hists"][
                                        "$filter"
                                    ]["cond"]["$and"][0]["$in"][
                                        1
                                    ] = filter_existing.permissions
                            try:
                                cursor = request.app["mongo"][
                                    filter_existing.catalog
                                ].aggregate(
                                    filter_template, allowDiskUse=False, maxTimeMS=3000
                                )
                                await cursor.to_list(length=None)
                                value["pipeline"] = pipeline
                            except Exception as e:
                                return self.error(
                                    message=f"Cannot update filter id {filter_id}: {modifiable_field} pipeline is invalid: {str(e)}"
                                )
                        else:
                            return self.error(
                                message=f"Cannot test {modifiable_field} pipeline: no documents in {filter_existing.catalog} collection"
                            )
                filter_doc[modifiable_field] = value
        filter_existing = Filter.parse_doc(filter_doc)

        await request.app["mongo_odm"].save(filter_existing)

        return self.success(
            message=f"Updated filter id {filter_id}", data=filter_existing.doc()
        )

    @admin_required
    async def delete(self, request: web.Request) -> web.Response:
        """Delete user-defined filter for (group_id, filter_id) altogether

        :param request:
        :return:

        ---
        summary: Delete user-defined filter by filter_id
        tags:
          - filters

        parameters:
          - in: query
            name: filter_id
            description: filter id
            required: true
            schema:
              type: integer
              minimum: 1

        responses:
          '200':
            description: filter removed
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [success]
                    message:
                      type: string
                example:
                  status: success
                  message: "Removed filter for group_id=1, filter_id=1"

          '400':
            description: filter not found or removal failed
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [error]
                    message:
                      type: string
                examples:
                  filter not found:
                    value:
                      status: error
                      message: Filter id 1 not found
        """
        filter_id = int(request.match_info["filter_id"])

        r = await request.app["mongo"].filters.delete_one({"filter_id": filter_id})

        if r.deleted_count != 0:
            return self.success(message=f"Removed filter id {filter_id}")

        return self.error(message=f"Filter id {filter_id} not found")


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


class ZTFTriggerHandler(Handler):
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


class ZTFMMATriggerHandler(Handler):
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


class ZTFObsHistoryHandler(Handler):
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


class SkymapHandlerPut(Model, ABC):
    """Data model for Skymap Handler for streamlined validation"""

    dateobs: Optional[Union[str, datetime.datetime]]
    trigger_id: Optional[int]
    aliases: Optional[List[str]]
    voevent: Optional[Union[str, bytes]]
    skymap: Optional[dict]

    contours: Union[List[int], List[float], int, float]


class SkymapHandlerGet(Model, ABC):
    dateobs: Optional[Union[str, datetime.datetime]]
    trigger_id: Optional[int]
    alias: Optional[str]
    localization_name: Optional[str]

    contours: Optional[Union[List[int], List[float], int, float]]


class SkymapHandlerDelete(Model, ABC):
    dateobs: Optional[Union[str, datetime.datetime]]
    trigger_id: Optional[int]
    alias: Optional[str]
    localization_name: str


class SkymapHandler(Handler):
    """Handler for users to upload skymaps and save their contours"""

    @auth_required
    async def put(self, request: web.Request) -> web.Response:
        """Save a skymap's contours at different levels, or add new contours to an existing skymap"""
        _data = await request.json()
        contour_levels = _data.get("contours", [90, 95])
        if isinstance(contour_levels, int) or isinstance(contour_levels, float):
            contour_levels = [contour_levels]
        elif isinstance(contour_levels, str):
            if "," in contour_levels:
                try:
                    contour_levels = [
                        str_to_numeric(contours_level)
                        for contours_level in contour_levels.split(",")
                    ]
                except ValueError:
                    raise ValueError(
                        "contours must be a comma-separated list of integers"
                    )
            else:
                try:
                    contour_levels = [str_to_numeric(contour_levels)]
                except ValueError:
                    raise ValueError(
                        "contours must be an integer or a comma-separated list of integers"
                    )

        try:
            SkymapHandlerPut(**_data)
        except ValidationError as e:
            return self.error(message=f"Invalid request body: {str(e)}")

        dateobs = _data.get("dateobs", None)
        if dateobs is None and "voevent" not in _data:
            raise ValueError("dateobs is required in the request body")
        if isinstance(dateobs, str):
            try:
                dateobs = datetime.datetime.strptime(dateobs, "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                try:
                    dateobs = datetime.datetime.strptime(dateobs, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    raise ValueError(
                        "dateobs must be in the format YYYY-MM-DDTHH:MM:SS[.SSSSSS] if it is a string"
                    )
        triggerid = _data.get("triggerid", None)
        aliases = _data.get("aliases", [])
        if isinstance(aliases, str):
            aliases = [aliases]
        if isinstance(aliases, list):
            try:
                aliases = [str(alias) for alias in aliases]
            except ValueError:
                raise ValueError("aliases must be strings")
        else:
            raise ValueError("aliases must be a list of strings")

        skymap = None
        contours = {}

        if "voevent" in _data:
            if voevent_schema.is_valid(_data["voevent"]):
                # check if is string
                try:
                    _data["voevent"] = _data["voevent"].encode("ascii")
                except AttributeError:
                    pass
                root = lxml.etree.fromstring(_data["voevent"])
            else:
                raise ValueError("xml file is not valid VOEvent")

            # DATEOBS
            dateobs = (
                get_dateobs(root)
                if _data.get("dateobs") is None
                else _data.get("dateobs")
            )
            if dateobs is None:
                raise ValueError(
                    "dateobs is required, either in the request body or in the VOEvent file if provided"
                )
            if isinstance(dateobs, str):
                try:
                    dateobs = datetime.datetime.strptime(
                        dateobs, "%Y-%m-%dT%H:%M:%S.%f"
                    )
                except ValueError:
                    try:
                        dateobs = datetime.datetime.strptime(
                            dateobs, "%Y-%m-%dT%H:%M:%S"
                        )
                    except ValueError:
                        raise ValueError(
                            "dateobs must be in the format YYYY-MM-DDTHH:MM:SS[.SSSSSS] if it is a string"
                        )

            # TRIGGERID
            triggerid = get_trigger(root)

            # ALIASES
            aliases = _data.get("aliases", [])
            if isinstance(aliases, str):
                aliases = [aliases]
            if isinstance(aliases, list):
                try:
                    aliases = [str(alias) for alias in aliases]
                except ValueError:
                    raise ValueError("aliases must be strings")
            else:
                raise ValueError("aliases must be a list of strings")
            voevent_aliases = get_aliases(root)
            if len(voevent_aliases) > 0:
                aliases.extend(voevent_aliases)

            # SKYMAP (from VOEvent)
            skymap = from_voevent(root)
            if skymap is None:
                raise ValueError("Could not get skymap from VOEvent file")

        elif "skymap" in _data and isinstance(_data["skymap"], dict):
            skymap_data = _data["skymap"]
            skymap = from_dict(skymap_data)
        else:
            raise ValueError(
                "either skymap dict or voevent is required in the request body"
            )

        # check if the skymap already exists

        query = {
            "$and": [
                {
                    "$or": [
                        {"dateobs": dateobs},
                        {"triggerid": triggerid},
                    ],
                },
                {"localization_name": skymap["localization_name"]},
            ]
        }
        if len(aliases) > 0:
            query["$and"][0]["$or"].append({"aliases": {"$all": aliases}})

        existing_skymap = await request.app["mongo"][
            config["database"]["collections"]["skymaps"]
        ].find_one(query)

        existing_contour_levels = []
        missing_contour_levels = []
        if existing_skymap is not None:
            existing_contour_levels = [
                str_to_numeric(level.replace("contour", ""))
                for level in existing_skymap.get("contours", {}).keys()
                if "contour" in level
            ]
            missing_contour_levels = [
                level
                for level in contour_levels
                if level not in existing_contour_levels
            ]
            if len(missing_contour_levels) == 0:
                return web.json_response(
                    {
                        "status": "already_exists",
                        "message": "skymap already exists with the same contours",
                        "data": {
                            "dateobs": dateobs.isoformat(),
                            "localization_name": skymap["localization_name"],
                            "contours": existing_contour_levels,
                        },
                    },
                    status=409,
                )
            else:
                contour_levels = missing_contour_levels

        # CONTOURS
        contours = get_contours(skymap, contour_levels)
        if contours is None:
            raise ValueError("Could not generate contours from skymap")

        if existing_skymap is not None:
            existing_contours = existing_skymap.get("contours", {})
            existing_contours.update(contours)
            contours = existing_contours
            try:
                # update the document in the database
                await request.app["mongo"][
                    config["database"]["collections"]["skymaps"]
                ].update_one(
                    {
                        "$and": [
                            {
                                "$or": [
                                    {"dateobs": dateobs},
                                    {"triggerid": triggerid},
                                    {"aliases": {"$all": aliases}},
                                ]
                            },
                            {"localization_name": skymap["localization_name"]},
                        ]
                    },
                    {"$set": {"contours": contours}},
                )
                return web.json_response(
                    {
                        "status": "success",
                        "message": f"updated skymap for {dateobs} to add contours {contour_levels}",
                        "data": {
                            "dateobs": dateobs.isoformat(),
                            "localization_name": skymap["localization_name"],
                            "contours": existing_contour_levels
                            + missing_contour_levels,
                        },
                    }
                )
            except Exception as e:
                return web.json_response({"status": "error", "message": str(e)})

        document = {
            "dateobs": dateobs,
            "aliases": aliases,
            "contours": contours,
            "localization_name": skymap["localization_name"],
        }
        if triggerid is not None:
            document["triggerid"] = triggerid

        try:
            # save skymap
            await request.app["mongo"][
                config["database"]["collections"]["skymaps"]
            ].insert_one(document)
            return web.json_response(
                {
                    "status": "success",
                    "message": f"added skymap for {dateobs} with contours {contour_levels}",
                    "data": {
                        "dateobs": dateobs.isoformat(),
                        "localization_name": skymap["localization_name"],
                        "contours": contour_levels,
                    },
                }
            )
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)})

    @auth_required
    async def get(self, request: web.Request) -> web.Response:
        """Retrieve a skymap using either a dateobs, triggerid, or alias"""

        try:
            SkymapHandlerGet(**request.query)
        except ValidationError as e:
            return web.json_response({"status": "error", "message": str(e)})

        query = {}
        if request.query.get("dateobs") is not None:
            dateobs = request.query["dateobs"]
            if isinstance(dateobs, str):
                try:
                    dateobs = datetime.datetime.strptime(
                        dateobs, "%Y-%m-%dT%H:%M:%S.%f"
                    )
                except ValueError:
                    try:
                        dateobs = datetime.datetime.strptime(
                            dateobs, "%Y-%m-%dT%H:%M:%S"
                        )
                    except ValueError:
                        raise ValueError(
                            "dateobs must be in the format YYYY-MM-DDTHH:MM:SS[.SSSSSS] if it is a string"
                        )
            query["dateobs"] = dateobs
        if request.query.get("triggerid") is not None:
            query["triggerid"] = request.query["triggerid"]
        if request.query.get("alias") is not None:
            query["aliases"] = request.query["alias"]

        if request.query.get("localization_name") is not None:
            query["localization_name"] = request.query["localization_name"]

        if len(query) == 0:
            return web.json_response(
                {
                    "status": "error",
                    "message": "must provide dateobs, triggerid, or alias",
                }
            )

        try:
            skymap = await request.app["mongo"][
                config["database"]["collections"]["skymaps"]
            ].find_one(query)
            if skymap is None:
                return web.json_response(
                    {"status": "error", "message": "no skymap found"}
                )
            if request.query.get("contours") is not None:
                contours = request.query["contours"]
                if isinstance(contours, int) or isinstance(contours, float):
                    contours = [contours]
                elif isinstance(contours, str):
                    if "," in contours:
                        try:
                            contours = [
                                str_to_numeric(contour)
                                for contour in contours.split(",")
                            ]
                        except ValueError:
                            raise ValueError(
                                "contours must be a comma-separated list of integers"
                            )
                    else:
                        try:
                            contours = [str_to_numeric(contours)]
                        except ValueError:
                            raise ValueError(
                                "contours must be an integer or a comma-separated list of integers"
                            )

                missing_contours = [
                    level
                    for level in contours
                    if f"contour{level}" not in skymap["contours"]
                ]
                if len(missing_contours) > 0:
                    return web.json_response(
                        {
                            "status": "error",
                            "message": f"skymap exists but is missing contours {missing_contours}",
                        }
                    )
            del skymap["_id"]
            skymap["dateobs"] = skymap["dateobs"].isoformat()
            return web.json_response({"status": "success", "data": skymap})
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)})

    @auth_required
    async def delete(self, request: web.Request) -> web.Response:
        """Delete a skymap using either a dateobs, triggerid, or alias"""
        _data = await request.json()
        try:
            SkymapHandlerDelete(**_data)
        except ValidationError as e:
            return web.json_response({"status": "error", "message": str(e)})

        query = {}
        if _data.get("dateobs") is not None:
            dateobs = _data["dateobs"]
            if isinstance(dateobs, str):
                try:
                    dateobs = datetime.datetime.strptime(
                        dateobs, "%Y-%m-%dT%H:%M:%S.%f"
                    )
                except ValueError:
                    try:
                        dateobs = datetime.datetime.strptime(
                            dateobs, "%Y-%m-%dT%H:%M:%S"
                        )
                    except ValueError:
                        raise ValueError(
                            "dateobs must be in the format YYYY-MM-DDTHH:MM:SS[.SSSSSS] if it is a string"
                        )
            query["dateobs"] = dateobs
        if _data.get("triggerid") is not None:
            query["triggerid"] = _data["triggerid"]
        if _data.get("alias") is not None:
            query["aliases"] = _data["alias"]

        if len(query) == 0:
            return web.json_response(
                {
                    "status": "error",
                    "message": "must provide dateobs, triggerid, or alias",
                }
            )

        query["localization_name"] = _data["localization_name"]

        try:
            result = await request.app["mongo"][
                config["database"]["collections"]["skymaps"]
            ].delete_one(query)
            if result.deleted_count == 0:
                return web.json_response(
                    {"status": "error", "message": "no skymap found"}
                )
            return web.json_response(
                {
                    "status": "success",
                    "message": f"deleted {result.deleted_count} skymap",
                }
            )
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)})


""" lab """


# @routes.get('/lab/ztf-alerts/{candid}/cutout/{cutout}/{file_format}', allow_head=False)
@auth_required
async def ztf_alert_get_cutout(request):
    """
    Serve ZTF alert cutouts as fits or png

    :param request:
    :return:

    ---
    summary: Serve ZTF alert cutout as fits or png
    tags:
      - lab

    parameters:
      - in: path
        name: candid
        description: "ZTF alert candid"
        required: true
        schema:
          type: integer
      - in: path
        name: cutout
        description: "retrieve science, template, or difference cutout image?"
        required: true
        schema:
          type: string
          enum: [science, template, difference]
      - in: path
        name: file_format
        description: "response file format: original lossless FITS or rendered png"
        required: true
        schema:
          type: string
          enum: [fits, png]
      - in: query
        name: interval
        description: "Interval to use when rendering png"
        required: false
        schema:
          type: string
          enum: [min_max, zscale]
      - in: query
        name: stretch
        description: "Stretch to use when rendering png"
        required: false
        schema:
          type: string
          enum: [linear,  asinh, sqrt]
      - in: query
        name: cmap
        description: "Color map to use when rendering png"
        required: false
        schema:
          type: string
          enum: [bone, gray, cividis, viridis, magma]

    responses:
      '200':
        description: retrieved cutout
        content:
          image/fits:
            schema:
              type: string
              format: binary
          image/png:
            schema:
              type: string
              format: binary

      '400':
        description: retrieval failed
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            example:
              status: error
              message: "failure: <error message>"
    """
    try:
        candid = int(request.match_info["candid"])
        cutout = request.match_info["cutout"].capitalize()
        file_format = request.match_info["file_format"]
        interval = request.query.get("interval")
        stretch = request.query.get("stretch")
        cmap = request.query.get("cmap", None)

        known_cutouts = ["Science", "Template", "Difference"]
        if cutout not in known_cutouts:
            return web.json_response(
                {
                    "status": "error",
                    "message": f"cutout {cutout} not in {str(known_cutouts)}",
                },
                status=400,
            )
        known_file_formats = ["fits", "png"]
        if file_format not in known_file_formats:
            return web.json_response(
                {
                    "status": "error",
                    "message": f"file format {file_format} not in {str(known_file_formats)}",
                },
                status=400,
            )

        normalization_methods = {
            "asymmetric_percentile": AsymmetricPercentileInterval(
                lower_percentile=1, upper_percentile=100
            ),
            "min_max": MinMaxInterval(),
            "zscale": ZScaleInterval(nsamples=600, contrast=0.045, krej=2.5),
        }
        if interval is None:
            interval = "asymmetric_percentile"
        normalizer = normalization_methods.get(
            interval.lower(),
            AsymmetricPercentileInterval(lower_percentile=1, upper_percentile=100),
        )

        stretching_methods = {
            "linear": LinearStretch,
            "log": LogStretch,
            "asinh": AsinhStretch,
            "sqrt": SqrtStretch,
        }
        if stretch is None:
            stretch = "log" if cutout != "Difference" else "linear"
        stretcher = stretching_methods.get(stretch.lower(), LogStretch)()

        if (cmap is None) or (
            cmap.lower() not in ["bone", "gray", "cividis", "viridis", "magma"]
        ):
            cmap = "bone"
        else:
            cmap = cmap.lower()

        alert = await request.app["mongo"]["ZTF_alerts"].find_one(
            {"candid": candid}, {f"cutout{cutout}": 1}, max_time_ms=10000
        )

        cutout_data = loads(dumps([alert[f"cutout{cutout}"]["stampData"]]))[0]

        # unzipped fits name
        fits_name = pathlib.Path(alert[f"cutout{cutout}"]["fileName"]).with_suffix("")

        # unzip and flip about y axis on the server side
        with gzip.open(io.BytesIO(cutout_data), "rb") as f:
            with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                header = hdu[0].header
                data_flipped_y = np.flipud(hdu[0].data)

        if file_format == "fits":
            hdu = fits.PrimaryHDU(data_flipped_y, header=header)
            # hdu = fits.PrimaryHDU(data_flipped_y)
            hdul = fits.HDUList([hdu])

            stamp_fits = io.BytesIO()
            hdul.writeto(fileobj=stamp_fits)

            return web.Response(
                body=stamp_fits.getvalue(),
                content_type="image/fits",
                headers=MultiDict(
                    {"Content-Disposition": f"Attachment;filename={fits_name}"}
                ),
            )

        if file_format == "png":
            buff = io.BytesIO()
            plt.close("all")
            fig = plt.figure()
            fig.set_size_inches(4, 4, forward=False)
            ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
            ax.set_axis_off()
            fig.add_axes(ax)

            # replace nans with median:
            img = np.array(data_flipped_y)
            # replace dubiously large values
            xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
            if img[xl].any():
                img[xl] = np.nan
            if np.isnan(img).any():
                median = float(np.nanmean(img.flatten()))
                img = np.nan_to_num(img, nan=median)

            norm = ImageNormalize(img, stretch=stretcher)
            img_norm = norm(img)
            vmin, vmax = normalizer.get_limits(img_norm)
            ax.imshow(img_norm, cmap=cmap, origin="lower", vmin=vmin, vmax=vmax)

            plt.savefig(buff, dpi=42)

            buff.seek(0)
            plt.close("all")
            return web.Response(body=buff, content_type="image/png")

    except Exception as _e:
        log(f"Got error: {str(_e)}")
        _err = traceback.format_exc()
        log(_err)
        return web.json_response(
            {"status": "error", "message": f"failure: {_err}"}, status=400
        )


# @routes.get('/lab/zuds-alerts/{candid}/cutout/{cutout}/{file_format}', allow_head=False)
@auth_required
async def zuds_alert_get_cutout(request):
    """
    Serve cutouts as fits or png

    :param request:
    :return:

    ---
    summary: Serve ZUDS alert cutout as fits or png
    tags:
      - lab

    parameters:
      - in: path
        name: candid
        description: "ZUDS alert candid"
        required: true
        schema:
          type: integer
      - in: path
        name: cutout
        description: "retrieve science, template, or difference cutout image?"
        required: true
        schema:
          type: string
          enum: [science, template, difference]
      - in: path
        name: file_format
        description: "response file format: original lossless FITS or rendered png"
        required: true
        schema:
          type: string
          enum: [fits, png]
      - in: query
        name: scaling
        description: "Scaling to use when rendering png"
        required: false
        schema:
          type: string
          enum: [linear,  arcsinh, zscale]
      - in: query
        name: cmap
        description: "Color map to use when rendering png"
        required: false
        schema:
          type: string
          enum: [bone, gray, cividis, viridis, magma]

    responses:
      '200':
        description: retrieved cutout
        content:
          image/fits:
            schema:
              type: string
              format: binary
          image/png:
            schema:
              type: string
              format: binary

      '400':
        description: retrieval failed
        content:
          application/json:
            schema:
              type: object
              required:
                - status
                - message
              properties:
                status:
                  type: string
                  enum: [error]
                message:
                  type: string
            example:
              status: error
              message: "failure: <error message>"
    """
    try:
        candid = int(request.match_info["candid"])
        cutout = request.match_info["cutout"].capitalize()
        file_format = request.match_info["file_format"]
        scaling = request.query.get("scaling", None)
        cmap = request.query.get("cmap", None)

        known_cutouts = ["Science", "Template", "Difference"]
        if cutout not in known_cutouts:
            return web.json_response(
                {
                    "status": "error",
                    "message": f"cutout {cutout} not in {str(known_cutouts)}",
                },
                status=400,
            )
        known_file_formats = ["fits", "png"]
        if file_format not in known_file_formats:
            return web.json_response(
                {
                    "status": "error",
                    "message": f"file format {file_format} not in {str(known_file_formats)}",
                },
                status=400,
            )

        default_scaling = {"Science": "log", "Template": "log", "Difference": "linear"}
        if (scaling is None) or (
            scaling.lower() not in ("log", "linear", "zscale", "arcsinh")
        ):
            scaling = default_scaling[cutout]
        else:
            scaling = scaling.lower()

        if (cmap is None) or (
            cmap.lower() not in ["bone", "gray", "cividis", "viridis", "magma"]
        ):
            cmap = "bone"
        else:
            cmap = cmap.lower()

        alert = await request.app["mongo"]["ZUDS_alerts"].find_one(
            {"candid": candid}, {f"cutout{cutout}": 1}, max_time_ms=60000
        )

        cutout_data = loads(dumps([alert[f"cutout{cutout}"]]))[0]

        # unzipped fits name
        fits_name = f"{candid}.cutout{cutout}.fits"

        # unzip and flip about y axis on the server side
        with gzip.open(io.BytesIO(cutout_data), "rb") as f:
            with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                header = hdu[0].header
                # no need to flip it since Danny does that on his end
                # data_flipped_y = np.flipud(hdu[0].data)
                data_flipped_y = hdu[0].data

        if file_format == "fits":
            hdu = fits.PrimaryHDU(data_flipped_y, header=header)
            # hdu = fits.PrimaryHDU(data_flipped_y)
            hdul = fits.HDUList([hdu])

            stamp_fits = io.BytesIO()
            hdul.writeto(fileobj=stamp_fits)

            return web.Response(
                body=stamp_fits.getvalue(),
                content_type="image/fits",
                headers=MultiDict(
                    {"Content-Disposition": f"Attachment;filename={fits_name}"}
                ),
            )

        if file_format == "png":
            buff = io.BytesIO()
            plt.close("all")
            fig = plt.figure()
            fig.set_size_inches(4, 4, forward=False)
            ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
            ax.set_axis_off()
            fig.add_axes(ax)

            # replace nans with median:
            img = np.array(data_flipped_y)
            # replace dubiously large values
            xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
            if img[xl].any():
                img[xl] = np.nan
            if np.isnan(img).any():
                median = float(np.nanmean(img.flatten()))
                img = np.nan_to_num(img, nan=median)

            if scaling == "log":
                img[img <= 0] = np.median(img)
                ax.imshow(img, cmap=cmap, norm=LogNorm(), origin="lower")
            elif scaling == "linear":
                ax.imshow(img, cmap=cmap, origin="lower")
            elif scaling == "zscale":
                interval = ZScaleInterval(
                    nsamples=img.shape[0] * img.shape[1], contrast=0.045, krej=2.5
                )
                limits = interval.get_limits(img)
                ax.imshow(
                    img, origin="lower", cmap=cmap, vmin=limits[0], vmax=limits[1]
                )
            elif scaling == "arcsinh":
                ax.imshow(np.arcsinh(img - np.median(img)), cmap=cmap, origin="lower")

            plt.savefig(buff, dpi=42)

            buff.seek(0)
            plt.close("all")
            return web.Response(body=buff, content_type="image/png")

    except Exception as _e:
        log(f"Got error: {str(_e)}")
        _err = traceback.format_exc()
        log(_err)
        return web.json_response(
            {"status": "error", "message": f"failure: {_err}"}, status=400
        )


async def app_factory():
    """
        App Factory
    :return:
    """
    # init db if necessary
    await init_db(config=config)

    # Database connection
    if config["database"]["srv"] is True:
        conn_string = "mongodb+srv://"
    else:
        conn_string = "mongodb://"

    if (
        config["database"]["admin_username"] is not None
        and config["database"]["admin_password"] is not None
    ):
        conn_string += f"{config['database']['admin_username']}:{config['database']['admin_password']}@"

    conn_string += f"{config['database']['host']}"
    if config["database"]["srv"] is not True:
        conn_string += f":{config['database']['port']}"

    if config["database"]["replica_set"] is not None:
        conn_string += f"/?replicaSet={config['database']['replica_set']}"

    client = AsyncIOMotorClient(
        conn_string,
        maxPoolSize=config["database"]["max_pool_size"],
    )
    mongo = client[config["database"]["db"]]

    # admin to connect to this instance from outside using API
    await add_admin(mongo, config=config)

    # app settings
    settings = {
        "client_max_size": config["server"].get("client_max_size", 1024**2),
    }

    # init app with auth and error handling middlewares
    app = web.Application(middlewares=[auth_middleware, error_middleware], **settings)

    # store mongo connection
    app["mongo"] = mongo

    # mark all enqueued tasks failed on startup
    await app["mongo"].queries.update_many(
        {"status": "enqueued"},
        {"$set": {"status": "error", "last_modified": datetime.datetime.utcnow()}},
    )

    # graciously close mongo client on shutdown
    async def close_mongo(_app):
        _app["mongo"].client.close()

    app.on_cleanup.append(close_mongo)

    # use ODMantic to work with structured data such as Filters
    engine = AIOEngine(client=client, database=config["database"]["db"])
    # ODM = Object Document Mapper
    app["mongo_odm"] = engine

    # set up JWT for user authentication/authorization
    app["JWT"] = {
        "JWT_SECRET": config["server"]["JWT_SECRET_KEY"],
        "JWT_ALGORITHM": config["server"]["JWT_ALGORITHM"],
        "JWT_EXP_DELTA_SECONDS": config["server"]["JWT_EXP_DELTA_SECONDS"],
    }

    # OpenAPI docs:
    s = SwaggerDocs(
        app,
        redoc_ui_settings=ReDocUiSettings(path="/docs/api/"),
        # swagger_ui_settings=SwaggerUiSettings(path="/docs/api/"),
        validate=config["misc"]["openapi_validate"],
        title=config["server"]["name"],
        version=config["server"]["version"],
        description=config["server"]["description"],
        components=os.path.join("kowalski/api/components_api.yaml"),  # TODO: verify
    )

    # instantiate handler classes:
    query_handler = QueryHandler()
    filter_handler = FilterHandler()
    ztf_trigger_handler = ZTFTriggerHandler()
    ztf_trigger_handler_test = ZTFTriggerHandler(test=True)
    ztf_mma_trigger_handler = ZTFMMATriggerHandler()
    ztf_mma_trigger_handler_test = ZTFMMATriggerHandler(test=True)
    skymap_handler = SkymapHandler()

    # add routes manually
    s.add_routes(
        [
            web.get("/", ping, name="root", allow_head=False),
            # auth:
            web.post("/api/auth", auth_post, name="auth"),
            # users:
            web.post("/api/users", users_post),
            web.delete("/api/users/{username}", users_delete),
            web.put("/api/users/{username}", users_put),
            # queries:
            web.post("/api/queries", query_handler.post),
            # filters:
            web.get(
                r"/api/filters/{filter_id:[0-9]+}", filter_handler.get, allow_head=False
            ),
            web.post("/api/filters", filter_handler.post),
            web.patch("/api/filters", filter_handler.patch),
            web.delete("/api/filters/{filter_id:[0-9]+}", filter_handler.delete),
            # triggers
            web.get("/api/triggers/ztf", ztf_trigger_handler.get),
            web.put("/api/triggers/ztf", ztf_trigger_handler.put),
            web.delete("/api/triggers/ztf", ztf_trigger_handler.delete),
            web.get("/api/triggers/ztf.test", ztf_trigger_handler_test.get),
            web.put("/api/triggers/ztf.test", ztf_trigger_handler_test.put),
            web.delete("/api/triggers/ztf.test", ztf_trigger_handler_test.delete),
            web.get("/api/triggers/ztfmma", ztf_mma_trigger_handler.get),
            web.put("/api/triggers/ztfmma", ztf_mma_trigger_handler.put),
            web.delete("/api/triggers/ztfmma", ztf_mma_trigger_handler.delete),
            web.get("/api/triggers/ztfmma.test", ztf_mma_trigger_handler_test.get),
            web.put("/api/triggers/ztfmma.test", ztf_mma_trigger_handler_test.put),
            web.delete(
                "/api/triggers/ztfmma.test", ztf_mma_trigger_handler_test.delete
            ),
            web.put("/api/skymap", skymap_handler.put),
            web.get("/api/skymap", skymap_handler.get),
            web.delete("/api/skymap", skymap_handler.delete),
            # lab:
            web.get(
                "/lab/ztf-alerts/{candid}/cutout/{cutout}/{file_format}",
                ztf_alert_get_cutout,
                allow_head=False,
            ),
            web.get(
                "/lab/zuds-alerts/{candid}/cutout/{cutout}/{file_format}",
                zuds_alert_get_cutout,
                allow_head=False,
            ),
        ]
    )

    return app


uvloop.install()

if __name__ == "__main__":

    web.run_app(app_factory(), port=config["server"]["port"])
