import datetime
from abc import ABC
from typing import List, Optional, Union

import lxml
import xmlschema
from aiohttp import web
from odmantic import Model
from pydantic import ValidationError

from kowalski.config import load_config
from kowalski.tools.gcn_utils import (
    from_dict,
    from_voevent,
    get_aliases,
    get_contours,
    get_dateobs,
    get_trigger,
)
from kowalski.utils import (
    str_to_numeric,
)

from kowalski.api.middlewares import (
    auth_required,
)
from .base import BaseHandler

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


voevent_schema = xmlschema.XMLSchema("schema/VOEvent-v2.0.xsd")


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


class SkymapHandler(BaseHandler):
    """Handler for users to upload skymaps and save their contours"""

    @auth_required
    async def put(self, request: web.Request) -> web.Response:
        """
        Save a skymap's contours at different levels, or add new contours to an existing skymap

        :param request:
        :return:

        ---
        summary: Save a skymap's contours at different levels, or add new contours to an existing skymap
        tags:
            - skymap
        requestBody:
            required: true
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            dateobs:
                                type: string
                                format: date-time
                            triggerid:
                                type: string
                            aliases:
                                type: array
                                items:
                                    type: string
                            voevent:
                                type: string
                            skymap:
                                type: object
                            contours:
                                type: array
                                items:
                                    type: number
        responses:
            '200':
                description: skymap contours saved successfully
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                                data:
                                    type: object
                                    properties:
                                        dateobs:
                                            type: string
                                            format: date-time
                                        localization_name:
                                            type: string
                                        contours:
                                            type: array
                                            items:
                                                type: number
                            example:
                                status: success
                                message: added skymap for 2021-01-01T00:00:00 with contours [90, 95]
                                data:
                                    dateobs: 2021-01-01T00:00:00
                                    localization_name: skymap_2021-01-01T00:00:00
                                    contours: [90, 95]
            '400':
                description: invalid request body
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: "Invalid request body: 1 validation error for SkymapHandlerPut"
                                data:
                                    dateobs
                                    aliases
                                    voevent
                                    skymap
                                    contours
            '409':
                description: skymap already exists with the same contours
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                                data:
                                    type: object
                                    properties:
                                        dateobs:
                                            type: string
                                            format: date-time
                                        localization_name:
                                            type: string
                                        contours:
                                            type: array
                                            items:
                                                type: number
                            example:
                                status: already_exists
                                message: skymap already exists with the same contours
                                data:
                                    dateobs: 2021-01-01T00:00:00
                                    localization_name: skymap_2021-01-01T00:00:00
                                    contours: [90, 95]
            '500':
                description: internal server error
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: internal server error
        """
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
                parser = lxml.etree.XMLParser(resolve_entities=False)
                root = lxml.etree.fromstring(_data["voevent"], parser)
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
                    ],
                },
                {"localization_name": skymap["localization_name"]},
            ]
        }
        if triggerid not in [None, ""]:
            query["$and"][0]["$or"].append({"triggerid": triggerid})
        if len(aliases) > 0:
            query["$and"][0]["$or"].append(
                {
                    "$and": [
                        {"aliases": {"$all": aliases}},
                        {"aliases": {"$size": len(aliases)}},
                    ]
                }
            )

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
                    {"_id": existing_skymap["_id"]},
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
        """
        Retrieve a skymap using either a dateobs, triggerid, or alias

        :param request:
        :return:

        ---
        summary: Retrieve a skymap using either a dateobs, triggerid, or alias
        tags:
            - skymap
        parameters:
            - in: query
              name: dateobs
              schema:
                type: string
                format: date-time
              description: dateobs of the skymap
            - in: query
              name: triggerid
              schema:
                type: string
              description: triggerid of the skymap
            - in: query
              name: alias
              schema:
                type: string
              description: alias of the skymap
            - in: query
              name: localization_name
              schema:
                type: string
              description: localization name of the skymap
            - in: query
              name: contours
              schema:
                type: string
              description: contours to retrieve
        responses:
            '200':
                description: skymap retrieved successfully
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                data:
                                    type: object
                                    properties:
                                        dateobs:
                                            type: string
                                            format: date-time
                                        localization_name:
                                            type: string
                                        contours:
                                            type: object
                            example:
                                status: success
                                data:
                                    dateobs: 2021-01-01T00:00:00
                                    localization_name: skymap_2021-01-01T00:00:00
                                    contours:
                                        contour90: "https://url/to/contour90"
                                        contour95: "https://url/to/contour95"
            '400':
                description: invalid request query
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: "must provide dateobs, triggerid, or alias"
            '404':
                description: skymap not found
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: "no skymap found"
            '500':
                description: internal server error
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: internal server error
        """

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
        """
        Delete a skymap using either a dateobs, triggerid, or alias

        :param request:
        :return:

        ---
        summary: Delete a skymap and localization using either a dateobs, triggerid, or alias
        tags:
            - skymap
        requestBody:
            required: true
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            dateobs:
                                type: string
                                format: date-time
                            triggerid:
                                type: string
                            alias:
                                type: string
                            localization_name:
                                type: string
        responses:
            '200':
                description: skymap deleted successfully
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: success
                                message: deleted 1 skymap
            '400':
                description: invalid request body
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: "must provide dateobs, triggerid, or alias"
            '404':
                description: skymap not found
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: "no skymap found"
            '500':
                description: internal server error
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                message:
                                    type: string
                            example:
                                status: error
                                message: internal server error
        """
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
