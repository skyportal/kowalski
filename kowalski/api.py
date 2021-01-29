from abc import ABC
import aiofiles
from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs, ReDocUiSettings
from astropy.io import fits
from astropy.visualization import (
    AsymmetricPercentileInterval,
    MinMaxInterval,
    ZScaleInterval,
    LinearStretch,
    LogStretch,
    AsinhStretch,
    SqrtStretch,
    ImageNormalize,
)
import asyncio
from ast import literal_eval
from bson.json_util import dumps, loads
import datetime
import gzip
import io
import jwt
from matplotlib.colors import LogNorm
import matplotlib.pyplot as plt
from middlewares import auth_middleware, error_middleware, auth_required, admin_required
from motor.motor_asyncio import AsyncIOMotorClient
from multidict import MultiDict
import numpy as np
from odmantic import AIOEngine, EmbeddedModel, Field, Model
import os
import pathlib
from pydantic import root_validator
import shutil
import traceback
from typing import List, Mapping, Optional
from utils import (
    add_admin,
    check_password_hash,
    compute_hash,
    generate_password_hash,
    init_db,
    load_config,
    log,
    radec_str2geojson,
    uid,
)
import uvloop


config = load_config(config_file="config.yaml")["kowalski"]


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
        except Exception as _e:
            log(_e)
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
                    "exp": datetime.datetime.utcnow()
                    + datetime.timedelta(
                        seconds=request.app["JWT"]["JWT_EXP_DELTA_SECONDS"]
                    ),
                }
                jwt_token = jwt.encode(
                    payload,
                    request.app["JWT"]["JWT_SECRET"],
                    request.app["JWT"]["JWT_ALGORITHM"],
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


def parse_query(task, save: bool = False):
    # save auxiliary stuff
    kwargs = task.get("kwargs", dict())

    # reduce!
    task_reduced = {"user": task["user"], "query": dict(), "kwargs": kwargs}

    prohibited_collections = ("users", "filters", "queries")

    if task["query_type"] == "estimated_document_count":
        # specify task type:
        task_reduced["query_type"] = "estimated_document_count"

        if task["user"] != config["server"]["admin_username"]:
            if str(task["query"]["catalog"]) in prohibited_collections:
                raise Exception("protected collection")

        task_reduced["query"]["catalog"] = task["query"]["catalog"]

    elif task["query_type"] == "find":
        # specify task type:
        task_reduced["query_type"] = "find"

        if task["user"] != config["server"]["admin_username"]:
            if str(task["query"]["catalog"]) in prohibited_collections:
                raise Exception("protected collection")

        task_reduced["query"]["catalog"] = task["query"]["catalog"]

        # construct filter
        _filter = task["query"]["filter"]
        if isinstance(_filter, str):
            # passed string? evaluate:
            catalog_filter = literal_eval(_filter.strip())
        elif isinstance(_filter, dict):
            # passed dict?
            catalog_filter = _filter
        else:
            raise ValueError("unsupported filter specification")

        task_reduced["query"]["filter"] = catalog_filter

        # construct projection
        if "projection" in task["query"]:
            _projection = task["query"]["projection"]
            if isinstance(_projection, str):
                # passed string? evaluate:
                catalog_projection = literal_eval(_projection.strip())
            elif isinstance(_projection, dict):
                # passed dict?
                catalog_projection = _projection
            else:
                raise ValueError("Unsupported projection specification")
        else:
            catalog_projection = dict()

        task_reduced["query"]["projection"] = catalog_projection

    elif task["query_type"] == "find_one":
        # specify task type:
        task_reduced["query_type"] = "find_one"

        if task["user"] != config["server"]["admin_username"]:
            if str(task["query"]["catalog"]) in prohibited_collections:
                raise Exception("protected collection")

        task_reduced["query"]["catalog"] = task["query"]["catalog"]

        # construct filter
        _filter = task["query"]["filter"]
        if isinstance(_filter, str):
            # passed string? evaluate:
            catalog_filter = literal_eval(_filter.strip())
        elif isinstance(_filter, dict):
            # passed dict?
            catalog_filter = _filter
        else:
            raise ValueError("Unsupported filter specification")

        task_reduced["query"]["filter"] = catalog_filter

    elif task["query_type"] == "count_documents":
        # specify task type:
        task_reduced["query_type"] = "count_documents"

        if task["user"] != config["server"]["admin_username"]:
            if str(task["query"]["catalog"]) in prohibited_collections:
                raise Exception("protected collection")

        task_reduced["query"]["catalog"] = task["query"]["catalog"]

        # construct filter
        _filter = task["query"]["filter"]
        if isinstance(_filter, str):
            # passed string? evaluate:
            catalog_filter = literal_eval(_filter.strip())
        elif isinstance(_filter, dict):
            # passed dict?
            catalog_filter = _filter
        else:
            raise ValueError("Unsupported filter specification")

        task_reduced["query"]["filter"] = catalog_filter

    elif task["query_type"] == "aggregate":
        # specify task type:
        task_reduced["query_type"] = "aggregate"

        if task["user"] != config["server"]["admin_username"]:
            if str(task["query"]["catalog"]) in prohibited_collections:
                raise Exception("protected collection")

        task_reduced["query"]["catalog"] = task["query"]["catalog"]

        # construct pipeline
        _pipeline = task["query"]["pipeline"]
        if isinstance(_pipeline, str):
            # passed string? evaluate:
            catalog_pipeline = literal_eval(_pipeline.strip())
        elif isinstance(_pipeline, list) or isinstance(_pipeline, tuple):
            # passed dict?
            catalog_pipeline = _pipeline
        else:
            raise ValueError("Unsupported pipeline specification")

        task_reduced["query"]["pipeline"] = catalog_pipeline

    elif task["query_type"] == "cone_search":
        # specify task type:
        task_reduced["query_type"] = "cone_search"

        # apply filter before positional query?
        filter_first = task_reduced["kwargs"].get("filter_first", False)

        # cone search radius:
        cone_search_radius = float(
            task["query"]["object_coordinates"]["cone_search_radius"]
        )
        # convert to rad:
        if task["query"]["object_coordinates"]["cone_search_unit"] == "arcsec":
            cone_search_radius *= np.pi / 180.0 / 3600.0
        elif task["query"]["object_coordinates"]["cone_search_unit"] == "arcmin":
            cone_search_radius *= np.pi / 180.0 / 60.0
        elif task["query"]["object_coordinates"]["cone_search_unit"] == "deg":
            cone_search_radius *= np.pi / 180.0
        elif task["query"]["object_coordinates"]["cone_search_unit"] == "rad":
            cone_search_radius *= 1
        else:
            raise Exception(
                'unknown cone search unit: must be in ["arcsec", "arcmin", "deg", "rad"]'
            )

        if isinstance(task["query"]["object_coordinates"]["radec"], str):
            radec = task["query"]["object_coordinates"]["radec"].strip()

            # comb radecs for a single source as per Tom's request:
            if radec[0] not in ("[", "(", "{"):
                ra, dec = radec.split()
                if ("s" in radec) or (":" in radec):
                    radec = f"[('{ra}', '{dec}')]"
                else:
                    radec = f"[({ra}, {dec})]"

            objects = literal_eval(radec)
        elif (
            isinstance(task["query"]["object_coordinates"]["radec"], list)
            or isinstance(task["query"]["object_coordinates"]["radec"], tuple)
            or isinstance(task["query"]["object_coordinates"]["radec"], dict)
        ):
            objects = task["query"]["object_coordinates"]["radec"]
        else:
            raise Exception("bad source coordinates")

        # this could either be list/tuple [(ra1, dec1), (ra2, dec2), ..] or dict {'name': (ra1, dec1), ...}
        if isinstance(objects, list) or isinstance(objects, tuple):
            object_coordinates = objects
            object_names = [
                str(obj_crd).replace(".", "_") for obj_crd in object_coordinates
            ]
        elif isinstance(objects, dict):
            object_names, object_coordinates = zip(*objects.items())
            object_names = list(map(str, object_names))
            object_names = [on.replace(".", "_") for on in object_names]
        else:
            raise ValueError("Unsupported object coordinates specs")

        for catalog in task["query"]["catalogs"]:
            task_reduced["query"][catalog] = dict()

            if task["user"] != config["server"]["admin_username"]:
                if str(catalog.strip()) in prohibited_collections:
                    raise Exception("Trying to query a protected collection")

            task_reduced["query"][catalog.strip()] = dict()

            # construct filter
            if "filter" in task["query"]["catalogs"][catalog]:
                _filter = task["query"]["catalogs"][catalog]["filter"]
                if isinstance(_filter, str):
                    # passed string? evaluate:
                    catalog_filter = literal_eval(_filter.strip())
                elif isinstance(_filter, dict):
                    # passed dict?
                    catalog_filter = _filter
                else:
                    raise ValueError("Unsupported filter specification")
            else:
                catalog_filter = dict()

            # construct projection
            if "projection" in task["query"]["catalogs"][catalog]:
                _projection = task["query"]["catalogs"][catalog]["projection"]
                if isinstance(_projection, str):
                    # passed string? evaluate:
                    catalog_projection = literal_eval(_projection.strip())
                elif isinstance(_projection, dict):
                    # passed dict?
                    catalog_projection = _projection
                else:
                    raise ValueError("Unsupported projection specification")
            else:
                catalog_projection = dict()

            # parse coordinate list

            for oi, obj_crd in enumerate(object_coordinates):
                # convert ra/dec into GeoJSON-friendly format
                _ra, _dec = radec_str2geojson(*obj_crd)
                object_position_query = dict()
                object_position_query["coordinates.radec_geojson"] = {
                    "$geoWithin": {"$centerSphere": [[_ra, _dec], cone_search_radius]}
                }
                # use stringified object coordinates as dict keys and merge dicts with cat/obj queries:

                if not filter_first:
                    task_reduced["query"][catalog][object_names[oi]] = (
                        {**object_position_query, **catalog_filter},
                        {**catalog_projection},
                    )
                else:
                    # place the filter expression in front of the positional query?
                    task_reduced["query"][catalog][object_names[oi]] = (
                        {**catalog_filter, **object_position_query},
                        {**catalog_projection},
                    )

    elif task["query_type"] == "info":

        # specify task type:
        task_reduced["query_type"] = "info"
        task_reduced["query"] = task["query"]

    if save:
        task_hashable = dumps(task_reduced)
        # compute hash for task. this is used as key in DB
        task_hash = compute_hash(task_hashable)

        # mark as enqueued in DB:
        t_stamp = datetime.datetime.utcnow()
        if "query_expiration_interval" not in kwargs:
            # default expiration interval:
            t_expires = t_stamp + datetime.timedelta(
                days=int(config["misc"]["query_expiration_interval"])
            )
        else:
            # custom expiration interval:
            t_expires = t_stamp + datetime.timedelta(
                days=int(kwargs["query_expiration_interval"])
            )

        # dump task_hashable to file, as potentially too big to store in mongo
        # save task:
        user_tmp_path = os.path.join(config["path"]["queries"], task["user"])
        # mkdir if necessary
        if not os.path.exists(user_tmp_path):
            os.makedirs(user_tmp_path)
        task_file = os.path.join(user_tmp_path, f"{task_hash}.task.json")

        with open(task_file, "w") as f_task_file:
            f_task_file.write(dumps(task))

        task_doc = {
            "task_id": task_hash,
            "user": task["user"],
            "task": task_file,
            "result": None,
            "status": "enqueued",
            "created": t_stamp,
            "expires": t_expires,
            "last_modified": t_stamp,
        }

        return task_hash, task_reduced, task_doc

    else:
        return "", task_reduced, {}


async def execute_query(mongo, task_hash, task_reduced, task_doc, save: bool = False):

    db = mongo

    if save:
        # mark query as enqueued:
        await db.queries.insert_one(task_doc)

    result = dict()
    query_result = None

    _query = task_reduced

    result["user"] = _query.get("user")
    result["kwargs"] = _query.get("kwargs", dict())

    # by default, long-running queries will be killed after config['misc']['max_time_ms'] ms
    max_time_ms = int(
        result["kwargs"].get("max_time_ms", config["misc"]["max_time_ms"])
    )
    assert max_time_ms >= 1, "bad max_time_ms, must be int>=1"

    try:

        # cone search:
        if _query["query_type"] == "cone_search":

            known_kwargs = ("skip", "hint", "limit", "sort")
            kwargs = {
                kk: vv for kk, vv in _query["kwargs"].items() if kk in known_kwargs
            }
            kwargs["comment"] = str(_query["user"])

            # iterate over catalogs as they represent
            query_result = dict()
            for catalog in _query["query"]:
                query_result[catalog] = dict()
                # iterate over objects:
                for obj in _query["query"][catalog]:
                    # project?
                    if len(_query["query"][catalog][obj][1]) > 0:
                        _select = db[catalog].find(
                            _query["query"][catalog][obj][0],
                            _query["query"][catalog][obj][1],
                            max_time_ms=max_time_ms,
                            **kwargs,
                        )
                    # return the whole documents by default
                    else:
                        _select = db[catalog].find(
                            _query["query"][catalog][obj][0],
                            max_time_ms=max_time_ms,
                            **kwargs,
                        )
                    # mongodb does not allow having dots in field names -> replace with underscores
                    query_result[catalog][
                        obj.replace(".", "_")
                    ] = await _select.to_list(length=None)

        # convenience general search subtypes:
        elif _query["query_type"] == "find":
            known_kwargs = ("skip", "hint", "limit", "sort")
            kwargs = {
                kk: vv for kk, vv in _query["kwargs"].items() if kk in known_kwargs
            }
            kwargs["comment"] = str(_query["user"])

            # project?
            if len(_query["query"]["projection"]) > 0:

                _select = db[_query["query"]["catalog"]].find(
                    _query["query"]["filter"],
                    _query["query"]["projection"],
                    max_time_ms=max_time_ms,
                    **kwargs,
                )
            # return the whole documents by default
            else:
                _select = db[_query["query"]["catalog"]].find(
                    _query["query"]["filter"], max_time_ms=max_time_ms, **kwargs
                )

            # todo: replace with inspect.iscoroutinefunction(object)?
            if (
                isinstance(_select, int)
                or isinstance(_select, float)
                or isinstance(_select, tuple)
                or isinstance(_select, list)
                or isinstance(_select, dict)
                or (_select is None)
            ):
                query_result = _select
            else:
                query_result = await _select.to_list(length=None)

        elif _query["query_type"] == "find_one":
            known_kwargs = ("skip", "hint", "limit", "sort")
            kwargs = {
                kk: vv for kk, vv in _query["kwargs"].items() if kk in known_kwargs
            }
            kwargs["comment"] = str(_query["user"])

            _select = db[_query["query"]["catalog"]].find_one(
                _query["query"]["filter"], max_time_ms=max_time_ms
            )

            query_result = await _select

        elif _query["query_type"] == "count_documents":
            known_kwargs = ("skip", "hint", "limit")
            kwargs = {
                kk: vv for kk, vv in _query["kwargs"].items() if kk in known_kwargs
            }
            kwargs["comment"] = str(_query["user"])

            _select = db[_query["query"]["catalog"]].count_documents(
                _query["query"]["filter"], maxTimeMS=max_time_ms
            )

            query_result = await _select

        elif _query["query_type"] == "estimated_document_count":
            known_kwargs = ("maxTimeMS",)
            kwargs = {
                kk: vv for kk, vv in _query["kwargs"].items() if kk in known_kwargs
            }
            kwargs["comment"] = str(_query["user"])

            _select = db[_query["query"]["catalog"]].estimated_document_count(
                maxTimeMS=max_time_ms
            )

            query_result = await _select

        elif _query["query_type"] == "aggregate":
            known_kwargs = ("allowDiskUse", "maxTimeMS", "batchSize")
            kwargs = {
                kk: vv for kk, vv in _query["kwargs"].items() if kk in known_kwargs
            }
            kwargs["comment"] = str(_query["user"])

            _select = db[_query["query"]["catalog"]].aggregate(
                _query["query"]["pipeline"], allowDiskUse=True, maxTimeMS=max_time_ms
            )

            query_result = await _select.to_list(length=None)

        elif _query["query_type"] == "info":
            # collection/catalog info
            if _query["query"]["command"] == "catalog_names":

                # get available catalog names
                catalogs = await db.list_collection_names()
                # exclude system collections
                catalogs_system = (
                    config["database"]["collections"]["users"],
                    config["database"]["collections"]["filters"],
                    config["database"]["collections"]["queries"],
                )

                query_result = [
                    c for c in sorted(catalogs)[::-1] if c not in catalogs_system
                ]

            elif _query["query"]["command"] == "catalog_info":
                catalog = _query["query"]["catalog"]

                stats = await db.command("collstats", catalog)

                query_result = stats

            elif _query["query"]["command"] == "index_info":
                catalog = _query["query"]["catalog"]

                stats = await db[catalog].index_information()

                query_result = stats

            elif _query["query"]["command"] == "db_info":
                stats = await db.command("dbstats")
                query_result = stats

        # success!
        result["status"] = "success"
        result["message"] = "query successfully executed"

        if not save:
            # dump result back
            result["data"] = query_result

        else:
            # save task result:
            user_tmp_path = os.path.join(config["path"]["queries"], _query["user"])
            # mkdir if necessary
            if not os.path.exists(user_tmp_path):
                os.makedirs(user_tmp_path)
            task_result_file = os.path.join(user_tmp_path, f"{task_hash}.result.json")

            # save location in db:
            result["result"] = task_result_file

            async with aiofiles.open(task_result_file, "w") as f_task_result_file:
                task_result = dumps(query_result)
                await f_task_result_file.write(task_result)

        # db book-keeping:
        if save:
            # mark query as done:
            await db.queries.update_one(
                {"user": _query["user"], "task_id": task_hash},
                {
                    "$set": {
                        "status": result["status"],
                        "last_modified": datetime.datetime.utcnow(),
                        "result": result["result"],
                    }
                },
            )

        return task_hash, result

    except Exception as e:
        log(f"Got error: {str(e)}")
        _err = traceback.format_exc()
        log(_err)

        # book-keeping:
        if save:
            # save task result with error message:
            user_tmp_path = os.path.join(config["path"]["queries"], _query["user"])
            # mkdir if necessary
            if not os.path.exists(user_tmp_path):
                os.makedirs(user_tmp_path)
            task_result_file = os.path.join(user_tmp_path, f"{task_hash}.result.json")

            # save location in db:
            result["status"] = "error"
            result["message"] = _err

            async with aiofiles.open(task_result_file, "w") as f_task_result_file:
                task_result = dumps(result)
                await f_task_result_file.write(task_result)

            # mark query as failed:
            await db.queries.update_one(
                {"user": _query["user"], "task_id": task_hash},
                {
                    "$set": {
                        "status": result["status"],
                        "last_modified": datetime.datetime.utcnow(),
                        "result": None,
                    }
                },
            )

        else:
            result["status"] = "error"
            result["message"] = _err

            return task_hash, result

        raise Exception("query failed badly")


# @routes.post('/api/queries')
@auth_required
async def queries_post(request: web.Request) -> web.Response:
    """
    Query Kowalski

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
                enum: [aggregate, cone_search, count_documents, estimated_document_count, find, find_one, info]
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

            estimated_document_count:
              value:
                "query_type": "estimated_document_count"
                "query": {
                  "catalog": "ZTF_alerts"
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
                  "user": "admin"
                  "kwargs": {
                    "max_time_ms": 2000
                  }
                  "status": "success"
                  "message": "query successfully executed"
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
                  "user": "admin"
                  "kwargs": {
                    "filter_first": false
                  }
                  "status": "success"
                  "message": "query successfully executed"
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
                  "user": "admin"
                  "kwargs": {
                    "sort": [["$natural", -1]],
                    "limit": 2
                  }
                  "status": "success"
                  "message": "query successfully executed"
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
                  "user": "admin"
                  "kwargs": {}
                  "status": "success"
                  "message": "query successfully executed"
                  "data": [
                    "ZTF_alerts_aux",
                    "ZTF_alerts"
                  ]

              count_documents:
                value:
                  "user": "admin"
                  "kwargs": {}
                  "status": "success"
                  "message": "query successfully executed"
                  "data": 1

              estimated_document_count:
                value:
                  "user": "admin"
                  "kwargs": {}
                  "status": "success"
                  "message": "query successfully executed"
                  "data": 11

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
    try:
        try:
            _query = await request.json()
        except Exception as _e:
            log(f"Cannot extract json() from request, trying post(): {str(_e)}")
            _query = await request.post()

        # parse query
        known_query_types = (
            "cone_search",
            "count_documents",
            "estimated_document_count",
            "find",
            "find_one",
            "aggregate",
            "info",
        )

        if _query["query_type"] not in known_query_types:
            return web.json_response(
                {
                    "status": "error",
                    "message": f'query_type {_query["query_type"]} not in {str(known_query_types)}',
                },
                status=400,
            )

        _query["user"] = request.user

        # by default, [unless enqueue_only is requested]
        # all queries are not registered in the db and the task/results are not stored on disk as json files
        # giving a significant execution speed up. this behaviour can be overridden.
        save = _query.get("kwargs", dict()).get("save", False)

        task_hash, task_reduced, task_doc = parse_query(_query, save=save)

        # execute query:
        if not save:
            task_hash, result = await execute_query(
                request.app["mongo"], task_hash, task_reduced, task_doc, save
            )

            return web.json_response(result, status=200, dumps=dumps)
        else:
            # only schedule query execution. store query and results, return query id to user
            asyncio.create_task(
                execute_query(
                    request.app["mongo"], task_hash, task_reduced, task_doc, save
                )
            )
            return web.json_response(
                {
                    "status": "success",
                    "query_id": task_hash,
                    "message": "query enqueued",
                },
                status=200,
                dumps=dumps,
            )

    except Exception as _e:
        log(f"Got error: {str(_e)}")
        _err = traceback.format_exc()
        log(_err)
        return web.json_response(
            {"status": "error", "message": f"failure: {_err}"}, status=400
        )


# @routes.get('/api/queries/{task_id}', allow_head=False)
@auth_required
async def queries_get(request):
    """
        Grab saved query / result.

    :return:
    """

    # get user:
    user = request.user

    # get query params
    task_id = request.match_info["task_id"]
    _data = request.query

    try:
        part = _data.get("part", "result")

        _query = await request.app["mongo"].queries.find_one(
            {"user": user, "task_id": {"$eq": task_id}},
        )

        if part == "task":
            task_file = os.path.join(
                config["path"]["queries"], user, f"{task_id}.task.json"
            )
            async with aiofiles.open(task_file, "r") as f_task_file:
                return web.json_response(await f_task_file.read(), status=200)

        elif part == "result":
            if _query["status"] == "enqueued":
                return web.json_response(
                    {"status": "success", "message": "query not finished yet"},
                    status=200,
                )

            task_result_file = os.path.join(
                config["path"]["queries"], user, f"{task_id}.result.json"
            )

            async with aiofiles.open(task_result_file, "r") as f_task_result_file:
                return web.json_response(await f_task_result_file.read(), status=200)

        else:
            return web.json_response(
                {"status": "error", "message": "part not recognized"}, status=400
            )

    except Exception as _e:
        log(f"Got error: {str(_e)}")
        _err = traceback.format_exc()
        log(_err)
        return web.json_response(
            {"status": "error", "message": f"failure: {_err}"}, status=500
        )


# @routes.delete('/api/queries/{task_id}')
@auth_required
async def queries_delete(request):
    """
        Delete saved query from DB programmatically.

    :return:
    """

    # get user:
    user = request.user

    # get query params
    task_id = request.match_info["task_id"]

    try:
        if task_id != "all":
            await request.app["mongo"].queries.delete_one(
                {"user": user, "task_id": {"$eq": task_id}}
            )

            # remove files containing task and result
            for p in pathlib.Path(os.path.join(config["path"]["queries"], user)).glob(
                f"{task_id}*"
            ):
                p.unlink()

        else:
            await request.app["mongo"].queries.delete_many({"user": user})

            # remove all files containing task and result
            if os.path.exists(os.path.join(config["path"]["queries"], user)):
                shutil.rmtree(os.path.join(config["path"]["queries"], user))

        return web.json_response(
            {"status": "success", "message": f"removed query: {task_id}"}, status=200
        )

    except Exception as _e:
        log(f"Got error: {str(_e)}")
        _err = traceback.format_exc()
        log(_err)
        return web.json_response(
            {"status": "error", "message": f"failure: {_err}"}, status=500
        )


""" filters """


FORBIDDEN_STAGES = {"$lookup", "$unionWith", "$out", "$merge"}


class FilterVersion(EmbeddedModel, ABC):
    """Data model for Filter versions"""

    fid: str = uid(length=6)
    pipeline: str
    created_at: datetime.datetime = datetime.datetime.utcnow()

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
        if len(stages.intersection(FORBIDDEN_STAGES)):
            raise ValueError(
                f"pipeline uses forbidden stages: {str(stages.intersection(FORBIDDEN_STAGES))}"
            )
        return values

    class Config:
        json_loads = loads
        json_dumps = dumps


class Filter(Model, ABC):
    """Data model for Filters"""

    filter_id: int = Field(ge=1)
    group_id: int = Field(ge=1)
    catalog: str
    permissions: List
    autosave: bool = False
    active: bool = True
    update_annotations: bool = False
    active_fid: Optional[str] = Field(min_length=6, max_length=6)
    fv: List[FilterVersion] = list()
    created_at: datetime.datetime = datetime.datetime.utcnow()
    last_modified: datetime.datetime = datetime.datetime.utcnow()

    class Config:
        # collection name in MongoDB
        collection = "filters"
        json_loads = loads
        json_dumps = dumps


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
                    description: "automatically save passing alerts to group <group_id>"
                    default: false
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
                # match permissions
                filter_template[0]["$match"]["candidate.programid"][
                    "$in"
                ] = filter_new.permissions
                filter_template[3]["$project"]["prv_candidates"]["$filter"]["cond"][
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
            test_successful, test_message = False, e
        if not test_successful:
            return self.error(message=test_message)

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
        summary: "Modify existing filters: activate/deactivate, set active_fid, autosave, or update_annotations"
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
                    description: "autosave candidates that pass filter to corresponding group?"
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
                filter_4:
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
                      message: filter id 1 not found
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
        filter_doc = filter_existing.doc()

        if filter_existing is None:
            return self.error(message=f"filter id {filter_id} not found")

        # note: partial model loading is not (yet?) available in odmantic + need a custom check on active_fid
        for modifiable_field in (
            "active",
            "active_fid",
            "autosave",
            "update_annotations",
        ):
            value = filter_spec.get(modifiable_field)
            if value is not None:
                if modifiable_field == "active_fid" and value not in [
                    filter_version["fid"] for filter_version in filter_doc["fv"]
                ]:
                    raise ValueError(
                        f"cannot set active_fid to {value}: filter version fid not in filter.fv"
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
                      message: filter for group_id=1, filter_id=1 not found
        """
        filter_id = int(request.match_info["filter_id"])

        r = await request.app["mongo"].filters.delete_one({"filter_id": filter_id})

        if r.deleted_count != 0:
            return self.success(message=f"Removed filter id {filter_id}")
        else:
            return self.error(message=f"Filter id {filter_id} not found")


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
          enum: [linear, log, asinh, sqrt]
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
            with fits.open(io.BytesIO(f.read())) as hdu:
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
          enum: [linear, log, arcsinh, zscale]
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
            with fits.open(io.BytesIO(f.read())) as hdu:
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
    mongodb_connection_string = (
        f"mongodb://{config['database']['username']}:{config['database']['password']}@"
        + f"{config['database']['host']}:{config['database']['port']}/{config['database']['db']}"
    )
    if config["database"]["replica_set"] is not None:
        mongodb_connection_string += f"?replicaSet={config['database']['replica_set']}"
    client = AsyncIOMotorClient(
        mongodb_connection_string,
        maxPoolSize=config["database"]["max_pool_size"],
    )
    mongo = client[config["database"]["db"]]

    # add site admin if necessary
    await add_admin(mongo, config=config)

    # init app with auth and error handling middlewares
    app = web.Application(middlewares=[auth_middleware, error_middleware])

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
    engine = AIOEngine(motor_client=client, database=config["database"]["db"])
    # ODM = Object Document Mapper
    app["mongo_odm"] = engine

    # set up JWT for user authentication/authorization
    app["JWT"] = {
        "JWT_SECRET": config["server"]["JWT_SECRET_KEY"],
        "JWT_ALGORITHM": config["server"]["JWT_ALGORITHM"],
        "JWT_EXP_DELTA_SECONDS": int(config["server"]["JWT_EXP_DELTA_SECONDS"]),
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
        components="components_api.yaml",
    )

    # instantiate handler classes:
    filter_handler = FilterHandler()

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
            web.post("/api/queries", queries_post),
            web.get("/api/queries/{task_id}", queries_get, allow_head=False),
            web.delete("/api/queries/{task_id}", queries_delete),
            # filters:
            web.get(
                r"/api/filters/{filter_id:[0-9]+}", filter_handler.get, allow_head=False
            ),
            web.post("/api/filters", filter_handler.post),
            web.patch("/api/filters", filter_handler.patch),
            web.delete("/api/filters/{filter_id:[0-9]+}", filter_handler.delete),
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
