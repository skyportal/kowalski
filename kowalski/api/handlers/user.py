import datetime
import traceback

import jwt
from aiohttp import web

from kowalski.config import load_config
from kowalski.log import log
from kowalski.utils import (
    check_password_hash,
    generate_password_hash,
)
from kowalski.api.middlewares import (
    admin_required,
)

from .base import BaseHandler

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


class UserHandler(BaseHandler):
    # @routes.post('/api/users')
    @admin_required
    async def post(self, request: web.Request) -> web.Response:
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
    async def delete(self, request: web.Request) -> web.Response:
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
                    {"status": "success", "message": f"removed user {username}"},
                    status=200,
                )
            else:
                return web.json_response(
                    {"status": "error", "message": f"user {username} not found"},
                    status=400,
                )

        except Exception as _e:
            return web.json_response(
                {"status": "error", "message": f"failed to remove user: {_e}"},
                status=500,
            )

    # @routes.put('/api/users/{username}')
    @admin_required
    async def put(self, request: web.Request) -> web.Response:
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


class UserTokenHandler(BaseHandler):
    # @routes.post('/api/auth')
    async def post(self, request: web.Request) -> web.Response:
        """
        Authentication

        ---
        summary: Get an access token for a user
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
                if (
                    penquins_version
                    not in config["misc"]["supported_penquins_versions"]
                ):
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
                if select is not None and check_password_hash(
                    select["password"], password
                ):
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
