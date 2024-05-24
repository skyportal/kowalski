import datetime
from abc import ABC
from typing import List, Optional, Union

from aiohttp import web
from bson.json_util import dumps, loads
from odmantic import EmbeddedModel, Field, Model
from pydantic import root_validator

from kowalski.config import load_config
from kowalski.log import log
from kowalski.utils import (
    uid,
)

from kowalski.api.middlewares import admin_required
from .base import BaseHandler

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

FORBIDDEN_STAGES_FILTERS = {"$lookup", "$unionWith", "$out", "$merge"}

# dict with the keys allowed in an filter's autosave section, and their data types
AUTOSAVE_KEYS = {
    "active": bool,
    "comment": str,
    "ignore_group_ids": list,
    "pipeline": list,
    "saver_id": int,
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


class FilterHandler(BaseHandler):
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
        """Post user-defined alert filter, or a new version
        - store pipeline as serialized extended json string,
          to be used with literal_eval to convert to dict at execution
        - run a simple sanity check before saving

        ---
        summary: Post user-defined alert filter, or a new version
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
                      "comment": "saved by blablabla",
                      "saver_id": 1,
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
