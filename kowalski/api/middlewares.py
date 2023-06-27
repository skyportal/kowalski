import traceback
from copy import deepcopy
from functools import wraps

import jwt
from aiohttp import web
from kowalski.log import log
from kowalski.config import load_config


config = load_config(config_files=["config.yaml"])["kowalski"]


@web.middleware
async def error_middleware(request, handler) -> web.Response:
    """Error handling middleware

    :param request:
    :param handler:
    :return:
    """
    try:
        response = await handler(request)
        if response.status in (200, 400, 409):
            return response
        error_message = response.reason
        status = response.status
    except web.HTTPException as ex:
        # catch "not found"
        if ex.status != 404:
            raise
        error_message = ex.reason
        status = ex.status
    except Exception as exception:
        # catch everything else
        log(traceback.format_exc())
        error_message = f"{type(exception).__name__}: {exception}"
        status = 400

    return web.json_response(
        {"status": "error", "message": error_message}, status=status
    )


@web.middleware
async def auth_middleware(request, handler) -> web.Response:
    """Auth middleware

    :param request:
    :param handler:
    :return:
    """
    request.user = None
    jwt_token = request.headers.get("authorization", None)

    if jwt_token:
        try:
            # accept both "Authorization: Bearer <token>" and "Authorization: <token>" headers
            if "bearer" in deepcopy(jwt_token).lower():
                jwt_token = jwt_token.split()[1]

            payload = jwt.decode(
                jwt_token,
                request.app["JWT"]["JWT_SECRET"],
                algorithms=[request.app["JWT"]["JWT_ALGORITHM"]],
            )
        except jwt.DecodeError:
            return web.json_response(
                {"status": "error", "message": "token is invalid"}, status=400
            )
        except jwt.ExpiredSignatureError:
            return web.json_response(
                {"status": "error", "message": "token has expired"}, status=400
            )

        request.user = payload["user_id"]

    response = await handler(request)

    return response


def auth_required(func):
    """Decorator to ensure successful user authorization to use the API

    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        request = args[-1]
        if not request.user:
            return web.json_response(
                {"status": "error", "message": "auth required"}, status=401
            )
        return func(*args, **kwargs)

    return wrapper


def admin_required(func):
    """Decorator to ensure user authorization _and_ admin rights

    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        request = args[-1]
        if not request.user:
            return web.json_response(
                {"status": "error", "message": "auth required"}, status=401
            )
        if request.user != config["server"]["admin_username"]:
            return web.json_response(
                {"status": "error", "message": "admin rights required"}, status=403
            )
        return func(*args, **kwargs)

    return wrapper
