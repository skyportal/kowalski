import datetime
from abc import ABC
from ast import literal_eval
from typing import Any, Mapping, Optional, Sequence, Union

import numpy as np
from aiohttp import web
from odmantic import Model
from pydantic import root_validator

from kowalski.config import load_config
from kowalski.utils import (
    radec_str2geojson,
)
from kowalski.api.middlewares import (
    auth_required,
)

from .base import BaseHandler, is_admin

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


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


class QueryHandler(BaseHandler):
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
                      - skymap
                  query:
                    type: object
                    description: Depends on query_type, see examples
                    oneOf:
                      - $ref: "#/components/schemas/aggregate"
                      - $ref: "#/components/schemas/cone_search"
                      - $ref: "#/components/schemas/count_documents"
                      - $ref: "#/components/schemas/estimated_document_count"
                      - $ref: "#/components/schemas/find"
                      - $ref: "#/components/schemas/find_one"
                      - $ref: "#/components/schemas/info"
                      - $ref: "#/components/schemas/near"
                      - $ref: "#/components/schemas/skymap"
                  kwargs:
                    type: object
                    description: Depends on query_type, see examples
                    oneOf:
                      - $ref: "#/components/schemas/aggregate_kwargs"
                      - $ref: "#/components/schemas/cone_search_kwargs"
                      - $ref: "#/components/schemas/count_documents_kwargs"
                      - $ref: "#/components/schemas/estimated_document_count_kwargs"
                      - $ref: "#/components/schemas/find_kwargs"
                      - $ref: "#/components/schemas/find_one_kwargs"
                      - $ref: "#/components/schemas/info_kwargs"
                      - $ref: "#/components/schemas/near_kwargs"
                      - $ref: "#/components/schemas/skymap_kwargs"
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

                skymap:
                  value:
                    "query_type": "skymap"
                    "query": {
                      "skymap": {
                        "dateobs": "2020-11-10T00:00:00.000",
                        "localization_name": "bayestar.fits",
                        "contour": 90
                      },
                      "catalog": "ZTF_alerts",
                      "filter": {"candidate.jd": {"$gte": 2459150.0, "$lt": 2459151.0}},
                      "projection": {"_id": 0, "candid": 1, "objectId": 1},
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

                  skymap:
                    value:
                      "status": "success"
                      "message": "Successfully executed query"
                      "data": [
                        {
                          "candid": 1127561444715015009,
                          "objectId": "ZTF20aaelulu"
                        }
                      ]

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
