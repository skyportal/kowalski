import gzip
import io
import pathlib
import traceback

import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
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
from aiohttp import web
from bson.json_util import dumps, loads
from multidict import MultiDict

from kowalski.config import load_config
from kowalski.log import log
from kowalski.api.middlewares import (
    auth_required,
)
from .base import BaseHandler

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


class AlertCutoutHandler(BaseHandler):
    @auth_required
    async def get(self, request: web.Request) -> web.Response:
        """
        Serve alert cutouts as fits or png

        :param request:
        :return:

        ---
        summary: Serve alert cutout as fits or png
        tags:
          - cutouts

        parameters:
          - in: path
            name: survey
            description: "The survey to retrieve cutout from"
            required: true
            schema:
              type: string
              enum: [ztf, wntr, pgir, turbo]
          - in: path
            name: candid
            description: "alert candid"
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
            survey = str(request.match_info["survey"]).lower()
            candid = int(request.match_info["candid"])
            cutout = str(request.match_info["cutout"]).lower()
            file_format = request.match_info["file_format"]
            interval = request.query.get("interval")
            stretch = request.query.get("stretch")
            cmap = request.query.get("cmap", None)

            if survey not in ["ztf", "wntr", "pgir", "turbo"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} must be one of [ztf, wntr, pgir, turbo]",
                    },
                    status=400,
                )

            collection_key = f"alerts_{survey}"
            if collection_key not in config["database"]["collections"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} not available",
                    },
                    status=400,
                )
            collection = config["database"]["collections"][collection_key]

            known_cutouts = ["science", "template", "difference"]
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

            cutout_key = (
                f"cutout{cutout.capitalize()}"
                if survey in ["ztf", "pgir", "turbo"]
                else f"cutout_{cutout}"
            )

            alert = await request.app["mongo"][collection].find_one(
                {"candid": candid}, {cutout_key: 1}, max_time_ms=10000
            )

            if alert is None:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"{survey} alert {candid} not found",
                    },
                    status=400,
                )

            if cutout_key not in alert:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"cutout {cutout} not found in {survey} alert {candid}",
                    },
                    status=400,
                )

            try:
                if survey in ["ztf", "turbo"]:
                    cutout_data = loads(dumps([alert[cutout_key]["stampData"]]))[0]
                else:
                    cutout_data = loads(dumps([alert[cutout_key]]))[0]
            except Exception as _e:
                log(f"Got error: {str(_e)}")
                _err = traceback.format_exc()
                log(_err)
                return web.json_response(
                    {"status": "error", "message": f"failure: {_err}"}, status=400
                )

            # unzipped fits name
            if survey in ["ztf", "turbo"]:
                fits_name = pathlib.Path(alert[cutout_key]["fileName"]).with_suffix("")
            else:
                fits_name = f"{survey}_{candid}_{cutout}.fits"

            # unzip and flip about y axis on the server side
            with gzip.open(io.BytesIO(cutout_data), "rb") as f:
                with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                    header = hdu[0].header
                    image_data = hdu[0].data

            # Survey-specific transformations to get North up and West on the right
            if survey in ["ztf", "wntr"]:
                image_data = np.flipud(image_data)
            elif survey in ["pgir"]:
                image_data = np.rot90(np.fliplr(image_data), 3)

            if file_format == "fits":
                hdu = fits.PrimaryHDU(image_data, header=header)
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
            elif file_format == "png":
                buff = io.BytesIO()
                plt.close("all")
                fig = plt.figure()
                fig.set_size_inches(4, 4, forward=False)
                ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
                ax.set_axis_off()
                fig.add_axes(ax)

                # replace nans with median:
                img = np.array(image_data)
                # replace dubiously large values
                xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
                if img[xl].any():
                    img[xl] = np.nan
                if np.isnan(img).any():
                    median = float(np.nanmean(img.flatten()))
                    img = np.nan_to_num(img, nan=median)

                stretching_methods = {
                    "linear": LinearStretch,
                    "log": LogStretch,
                    "asinh": AsinhStretch,
                    "sqrt": SqrtStretch,
                }
                if stretch is None:
                    stretch = "log" if cutout != "difference" else "linear"
                stretcher = stretching_methods.get(stretch.lower(), LogStretch)()

                normalization_methods = {
                    "asymmetric_percentile": AsymmetricPercentileInterval(
                        lower_percentile=1, upper_percentile=100
                    ),
                    "min_max": MinMaxInterval(),
                    "zscale": ZScaleInterval(n_samples=600, contrast=0.045, krej=2.5),
                }
                if interval is None:
                    interval = "asymmetric_percentile"
                normalizer = normalization_methods.get(
                    interval.lower(),
                    AsymmetricPercentileInterval(
                        lower_percentile=1, upper_percentile=100
                    ),
                )

                if (cmap is None) or (
                    cmap.lower() not in ["bone", "gray", "cividis", "viridis", "magma"]
                ):
                    cmap = "bone"
                else:
                    cmap = cmap.lower()

                norm = ImageNormalize(img, stretch=stretcher)
                img_norm = norm(img)
                vmin, vmax = normalizer.get_limits(img_norm)
                ax.imshow(img_norm, cmap=cmap, origin="lower", vmin=vmin, vmax=vmax)
                plt.savefig(buff, dpi=42)

                buff.seek(0)
                plt.close("all")

                return web.Response(body=buff, content_type="image/png")
            else:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"file format {file_format} not in {str(known_file_formats)}",
                    },
                    status=400,
                )
        except Exception as _e:
            log(f"Got error: {str(_e)}")
            _err = traceback.format_exc()
            log(_err)
            return web.json_response(
                {"status": "error", "message": f"failure: {_err}"}, status=400
            )


def prepare_classifications(classifications):
    if classifications is None:
        return {}
    if isinstance(classifications, str):
        classifications = {str(c): 1.0 for c in classifications.split(",")}
    elif isinstance(classifications, list):
        classifications = {str(c): 1.0 for c in classifications}
    elif isinstance(classifications, dict):
        classifications = {str(c): float(classifications[c]) for c in classifications}

    # sort alphabetically for consistency
    classifications = dict(sorted(classifications.items(), key=lambda item: item[0]))
    return classifications


def prepare_classifications_delete(classifications):
    if classifications is None:
        return []
    if isinstance(classifications, str):
        classifications = [str(c) for c in classifications.split(",")]
    elif isinstance(classifications, list):
        classifications = [str(c) for c in classifications]

    return classifications


class AlertClassificationHandler(BaseHandler):
    # take a dict with candids as key, and classifications as values
    async def put(self, request: web.Request) -> web.Response:
        """
        Add classifications for a list of alerts

        :param request:
        :return:

        ---
        summary: Add classifications for a list of alerts
        tags:
          - classifications

        parameters:
          - in: path
            name: survey
            description: "The survey to add classifications to"
            required: true
            schema:
              type: string
              enum: [ztf, wntr, pgir, turbo]

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
                additionalProperties:
                  type: object
                  properties:
                    human_labels:
                      type: object
                      additionalProperties:
                        type: string
                    ml_scores:
                      type: object
                      additionalProperties:
                        type: number
              example:
                "123456789": {
                  "human_labels": {
                  "label1": "label1",
                  "label2": "label2"
                  },
                  "ml_scores": {
                    "score1": 0.1,
                    "score2": 0.2
                  }
                }
        responses:
          '200':
            description: added classifications
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
                  message: "added classifications for ztf alerts"
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
            survey = str(request.match_info["survey"]).lower()
            if survey not in ["ztf", "wntr", "pgir", "turbo"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} must be one of [ztf, wntr, pgir, turbo]",
                    },
                    status=400,
                )

            candids = await request.json()
            if candids is None or not isinstance(candids, dict) or len(candids) == 0:
                return web.json_response(
                    {
                        "status": "error",
                        "message": "no candid provided",
                    },
                    status=400,
                )
            candids = {int(k): v for k, v in candids.items()}
            for c in candids:
                # human-based classifications, labels
                labels = candids[c].get("human_labels", None)
                labels = prepare_classifications(labels)

                # machine-based classifications, scores
                scores = candids[c].get("ml_scores", None)
                scores = prepare_classifications(scores)

                # check that at least one classification is provided
                if len(labels) == 0 and len(scores) == 0:
                    return web.json_response(
                        {
                            "status": "error",
                            "message": "no classifications provided, must provide at least one of human_labels or ml_scores",
                        },
                        status=400,
                    )

            alert_collection_key = f"alerts_{survey}"
            if alert_collection_key not in config["database"]["collections"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} not available",
                    },
                    status=400,
                )
            alert_collection = config["database"]["collections"][alert_collection_key]

            classification_collection_key = f"alerts_{survey}_classifications"
            if classification_collection_key not in config["database"]["collections"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} classifications not available",
                    },
                    status=400,
                )
            classification_collection = config["database"]["collections"][
                classification_collection_key
            ]

            # don't capitalize the I of objectId for WNTR
            obj_id_key = (
                "objectId" if survey in ["ztf", "turbo", "pgir"] else "objectid"
            )
            # verify that the alerts exists, get their objectId
            alerts = request.app["mongo"][alert_collection].find(
                {"candid": {"$in": list(candids.keys())}},
                {obj_id_key: 1, "candid": 1},
                max_time_ms=10000,
            )
            alerts = [a async for a in alerts]
            if len(alerts) < len(candids):
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"not all alerts found in {survey} collection",
                    },
                    status=400,
                )

            for a in alerts:
                c = a["candid"]
                doc = {
                    "_id": c,
                    obj_id_key: a[obj_id_key],
                }
                if "human_labels" in candids[c]:
                    doc = {
                        **doc,
                        **{
                            f"human_labels.{k}": labels[k]
                            for k in candids[c]["human_labels"]
                        },
                    }
                if "ml_scores" in candids[c]:
                    doc = {
                        **doc,
                        **{
                            f"ml_scores.{k}": scores[k] for k in candids[c]["ml_scores"]
                        },
                    }

                # if the document exists, update it, otherwise insert it
                await request.app["mongo"][classification_collection].update_one(
                    {"_id": c},
                    {"$set": doc},
                    upsert=True,
                )
        except Exception as _e:
            log(f"Got error: {str(_e)}")
            _err = traceback.format_exc()
            log(_err)
            return web.json_response(
                {"status": "error", "message": f"failure: {_err}"}, status=400
            )

        return web.json_response(
            {
                "status": "success",
                "message": f"added classifications for {survey} alerts",
            },
        )

    # delete classifications for a list of alerts. Must specify a list
    # of classifications to delete for each candid
    async def delete(self, request: web.Request) -> web.Response:
        """
        Delete classifications for a list of alerts

        :param request:
        :return:

        ---
        summary: Delete classifications for a list of alerts
        tags:
          - classifications

        parameters:
          - in: path
            name: survey
            description: "The survey to delete classifications from"
            required: true
            schema:
              type: string
              enum: [ztf, wntr, pgir, turbo]

        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
                additionalProperties:
                  type: object
                  properties:
                    human_labels:
                      type: array
                      items:
                        type: string
                    ml_scores:
                      type: array
                      items:
                        type: string
              example:
                "123456789": {
                 "human_labels": ["label1", "label2"],
                 "ml_scores": ["score1", "score2"]
                }
        responses:
          '200':
            description: deleted classifications
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
                  message: "deleted classifications for ztf alerts"
          '400':
            description: delete failed
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
            survey = str(request.match_info["survey"]).lower()
            if survey not in ["ztf", "wntr", "pgir", "turbo"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} must be one of [ztf, wntr, pgir, turbo]",
                    },
                    status=400,
                )
            candids = await request.json()
            if candids is None or not isinstance(candids, dict) or len(candids) == 0:
                return web.json_response(
                    {
                        "status": "error",
                        "message": "no candid provided",
                    },
                    status=400,
                )
            candids = {int(k): v for k, v in candids.items()}
            for c in candids:
                # human-based classifications, labels
                labels = candids[c].get("human_labels", None)
                labels = prepare_classifications_delete(labels)

                # machine-based classifications, scores
                scores = candids[c].get("ml_scores", None)
                scores = prepare_classifications_delete(scores)

                # check that at least one classification is provided
                if len(labels) == 0 and len(scores) == 0:
                    return web.json_response(
                        {
                            "status": "error",
                            "message": "no classifications provided, must provide at least one of human_labels or ml_scores",
                        },
                        status=400,
                    )

            classification_collection_key = f"alerts_{survey}_classifications"
            if classification_collection_key not in config["database"]["collections"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} classifications not available",
                    },
                    status=400,
                )
            classification_collection = config["database"]["collections"][
                classification_collection_key
            ]

            # verify that the classifications exists, get their objectId
            existing_classifications = request.app["mongo"][
                classification_collection
            ].find(
                {"_id": {"$in": list(candids.keys())}}, {"_id": 1}, max_time_ms=10000
            )
            existing_classifications = [c async for c in existing_classifications]
            if len(existing_classifications) == 0:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"not all classifications found in {survey} collection",
                    },
                    status=400,
                )

            # for the existing classifications, delete the keys
            for c in existing_classifications:
                c = c["_id"]
                doc = {}
                if len(labels) > 0:
                    doc = {**doc, **{f"human_labels.{k}": 1 for k in labels}}
                if len(scores) > 0:
                    doc = {**doc, **{f"ml_scores.{k}": 1 for k in scores}}

                # if the document exists, update it, otherwise insert it
                await request.app["mongo"][classification_collection].update_one(
                    {"_id": c},
                    {"$unset": doc},
                )

        except Exception as _e:
            log(f"Got error: {str(_e)}")
            _err = traceback.format_exc()
            log(_err)
            return web.json_response(
                {"status": "error", "message": f"failure: {_err}"}, status=400
            )

        return web.json_response(
            {
                "status": "success",
                "message": f"deleted classifications for {survey} alerts",
            },
        )

    # in principle we expect users to use penquins to query the classifications
    # but we can provide a simple interface to retrieve the classifications
    # for a given candid, mainly for testing purposes
    async def get(self, request: web.Request) -> web.Response:
        """
        Get classifications for a single alert

        :param request:
        :return:

        ---
        summary: Get classifications for a single alert
        tags:
          - classifications

        parameters:
          - in: path
            name: survey
            description: "The survey to retrieve classifications from"
            required: true
            schema:
              type: string
              enum: [ztf, wntr, pgir, turbo]
          - in: path
            name: candid
            description: "alert candid"
            required: true
            schema:
              type: integer
        responses:
          '200':
            description: retrieved classifications
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - data
                  properties:
                    status:
                      type: string
                      enum: [success]
                    data:
                      description: "classification data"
                      type: object
                      properties:
                        candid:
                          type: integer
                        objectId:
                          type: string
                        human_labels:
                          type: object
                          additionalProperties:
                            type: string
                        ml_scores:
                          type: object
                          additionalProperties:
                            type: string
                example:
                  status: success
                  data:
                    candid: 123456789
                    objectId: ZTF21aabcxyz
                    human_labels:
                      label1: 1
                      label2: 1
                    ml_scores:
                      score1: 0.1
                      score2: 0.2
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
            survey = str(request.match_info["survey"]).lower()
            if survey not in ["ztf", "wntr", "pgir", "turbo"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} must be one of [ztf, wntr, pgir, turbo]",
                    },
                    status=400,
                )

            candid = int(request.match_info["candid"])
            classification_collection_key = f"alerts_{survey}_classifications"
            if classification_collection_key not in config["database"]["collections"]:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"survey {survey} classifications not available",
                    },
                    status=400,
                )
            classification_collection = config["database"]["collections"][
                classification_collection_key
            ]

            classification = await request.app["mongo"][
                classification_collection
            ].find_one({"_id": candid}, max_time_ms=10000)
            if classification is None:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"classifications for {survey} alert {candid} not found",
                    },
                    status=400,
                )
            classification["candid"] = classification.pop("_id")
            return web.json_response(
                {
                    "status": "success",
                    "data": classification,
                },
            )
        except Exception as _e:
            log(f"Got error: {str(_e)}")
            _err = traceback.format_exc()
            log(_err)
            return web.json_response(
                {"status": "error", "message": f"failure: {_err}"}, status=400
            )
