import argparse
from ast import literal_eval
from astropy.io import fits
from astropy.visualization import (
    AsymmetricPercentileInterval,
    LinearStretch,
    LogStretch,
    ImageNormalize,
)
import base64
from bson.json_util import loads
import confluent_kafka
from copy import deepcopy
import datetime
import fastavro
import gzip
import io
import matplotlib.pyplot as plt
import multiprocessing
import numpy as np
import os
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import subprocess
import sys
from tensorflow.keras.models import load_model
import time
import traceback

from utils import (
    deg2dms,
    deg2hms,
    great_circle_distance,
    in_ellipse,
    load_config,
    log,
    Mongo,
    radec2lb,
    time_stamp,
)


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


DEFAULT_TIMEOUT = 5  # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


def read_schema_data(bytes_io):
    """Read data that already has an Avro schema.

    Parameters
    ----------
    bytes_io : `_io.BytesIO`
        Data to be decoded.

    Returns
    -------
    `dict`
        Decoded data.
    """
    bytes_io.seek(0)
    message = fastavro.reader(bytes_io)
    return message


class EopError(Exception):
    """
        Exception raised when reaching end of partition.

    Parameters
    ----------
    msg : Kafka message
        The Kafka message result from consumer.poll().
    """

    def __init__(self, msg):
        message = (
            f"{time_stamp()}: topic:{msg.topic()}, partition:{msg.partition()}, "
            f"status:end, offset:{msg.offset()}, key:{str(msg.key())}\n"
        )
        self.message = message

    def __str__(self):
        return self.message


def make_photometry(alert: dict, jd_start: float = None):
    """Make a de-duplicated pandas.DataFrame with photometry of alert['objectId']

    :param alert: ZTF alert packet/dict
    :param jd_start:
    :return:
    """
    alert = deepcopy(alert)
    df_candidate = pd.DataFrame(alert["candidate"], index=[0])

    df_prv_candidates = pd.DataFrame(alert["prv_candidates"])
    df_light_curve = pd.concat(
        [df_candidate, df_prv_candidates], ignore_index=True, sort=False
    )

    ztf_filters = {1: "ztfg", 2: "ztfr", 3: "ztfi"}
    df_light_curve["ztf_filter"] = df_light_curve["fid"].apply(lambda x: ztf_filters[x])
    df_light_curve["magsys"] = "ab"
    df_light_curve["mjd"] = df_light_curve["jd"] - 2400000.5

    df_light_curve["mjd"] = df_light_curve["mjd"].apply(lambda x: np.float64(x))
    df_light_curve["magpsf"] = df_light_curve["magpsf"].apply(lambda x: np.float32(x))
    df_light_curve["sigmapsf"] = df_light_curve["sigmapsf"].apply(
        lambda x: np.float32(x)
    )

    df_light_curve = (
        df_light_curve.drop_duplicates(subset=["mjd", "magpsf"])
        .reset_index(drop=True)
        .sort_values(by=["mjd"])
    )

    # filter out bad data:
    mask_good_diffmaglim = df_light_curve["diffmaglim"] > 0
    df_light_curve = df_light_curve.loc[mask_good_diffmaglim]

    # only "new" photometry requested?
    if jd_start is not None:
        w_after_jd = df_light_curve["jd"] > jd_start
        df_light_curve = df_light_curve.loc[w_after_jd]

    return df_light_curve


def make_thumbnail(alert, ttype: str, ztftype: str):
    """Convert lossless FITS cutouts from ZTF alerts into PNGs

    :param alert: ZTF alert packet/dict
    :param ttype: <new|ref|sub>
    :param ztftype: <Science|Template|Difference>
    :return:
    """
    alert = deepcopy(alert)

    cutout_data = alert[f"cutout{ztftype}"]["stampData"]
    with gzip.open(io.BytesIO(cutout_data), "rb") as f:
        with fits.open(io.BytesIO(f.read())) as hdu:
            # header = hdu[0].header
            data_flipped_y = np.flipud(hdu[0].data)
    # fixme: png, switch to fits eventually
    buff = io.BytesIO()
    plt.close("all")
    fig = plt.figure()
    fig.set_size_inches(4, 4, forward=False)
    ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()
    fig.add_axes(ax)

    # remove nans:
    img = np.array(data_flipped_y)
    img = np.nan_to_num(img)

    norm = ImageNormalize(
        img, stretch=LinearStretch() if ztftype == "Difference" else LogStretch()
    )
    img_norm = norm(img)
    normalizer = AsymmetricPercentileInterval(lower_percentile=1, upper_percentile=100)
    vmin, vmax = normalizer.get_limits(img_norm)
    ax.imshow(img_norm, cmap="bone", origin="lower", vmin=vmin, vmax=vmax)
    plt.savefig(buff, dpi=42)

    buff.seek(0)
    plt.close("all")

    thumb = {
        "obj_id": alert["objectId"],
        "data": base64.b64encode(buff.read()).decode("utf-8"),
        "ttype": ttype,
    }

    return thumb


""" Alert filters """


def make_triplet(alert, to_tpu: bool = False):
    """Make an L2-normalized cutout triplet out of a ZTF alert

    :param alert:
    :param to_tpu:
    :return:
    """
    cutout_dict = dict()

    for cutout in ("science", "template", "difference"):
        cutout_data = alert[f"cutout{cutout.capitalize()}"]["stampData"]

        # unzip
        with gzip.open(io.BytesIO(cutout_data), "rb") as f:
            with fits.open(io.BytesIO(f.read())) as hdu:
                data = hdu[0].data
                # replace nans with zeros
                cutout_dict[cutout] = np.nan_to_num(data)
                # L2-normalize
                cutout_dict[cutout] /= np.linalg.norm(cutout_dict[cutout])

        # pad to 63x63 if smaller
        shape = cutout_dict[cutout].shape
        if shape != (63, 63):
            # print(f'Shape of {candid}/{cutout}: {shape}, padding to (63, 63)')
            cutout_dict[cutout] = np.pad(
                cutout_dict[cutout],
                [(0, 63 - shape[0]), (0, 63 - shape[1])],
                mode="constant",
                constant_values=1e-9,
            )

    triplet = np.zeros((63, 63, 3))
    triplet[:, :, 0] = cutout_dict["science"]
    triplet[:, :, 1] = cutout_dict["template"]
    triplet[:, :, 2] = cutout_dict["difference"]

    if to_tpu:
        # Edge TPUs require additional processing
        triplet = np.rint(triplet * 128 + 128).astype(np.uint8).flatten()

    return triplet


def alert_filter__ml(alert, ml_models: dict = None) -> dict:
    """Execute ML models on ZTF alerts

    :param alert:
    :param ml_models:
    :return:
    """

    scores = dict()

    try:
        """ braai """
        triplet = make_triplet(alert)
        triplets = np.expand_dims(triplet, axis=0)
        braai = ml_models["braai"]["model"].predict(x=triplets)[0]
        # braai = 1.0
        scores["braai"] = float(braai)
        scores["braai_version"] = ml_models["braai"]["version"]
    except Exception as e:
        print(time_stamp(), str(e))

    return scores


# cone search radius:
cone_search_radius = float(config["database"]["xmatch"]["cone_search_radius"])
# convert to rad:
if config["database"]["xmatch"]["cone_search_unit"] == "arcsec":
    cone_search_radius *= np.pi / 180.0 / 3600.0
elif config["database"]["xmatch"]["cone_search_unit"] == "arcmin":
    cone_search_radius *= np.pi / 180.0 / 60.0
elif config["database"]["xmatch"]["cone_search_unit"] == "deg":
    cone_search_radius *= np.pi / 180.0
elif config["database"]["xmatch"]["cone_search_unit"] == "rad":
    cone_search_radius *= 1
else:
    raise Exception("Unknown cone search unit. Must be in [deg, rad, arcsec, arcmin]")


def alert_filter__xmatch(database, alert) -> dict:
    """
    Cross-match alerts
    """

    xmatches = dict()

    try:
        ra_geojson = float(alert["candidate"]["ra"])
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(alert["candidate"]["dec"])

        """ catalogs """
        for catalog in config["database"]["xmatch"]["catalogs"]:
            catalog_filter = config["database"]["xmatch"]["catalogs"][catalog]["filter"]
            catalog_projection = config["database"]["xmatch"]["catalogs"][catalog][
                "projection"
            ]

            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [[ra_geojson, dec_geojson], cone_search_radius]
                }
            }
            s = database[catalog].find(
                {**object_position_query, **catalog_filter}, {**catalog_projection}
            )
            xmatches[catalog] = list(s)

    except Exception as e:
        print(time_stamp(), str(e))

    return xmatches


# cone search radius in deg:
cone_search_radius_clu = 3.0
# convert deg to rad:
cone_search_radius_clu *= np.pi / 180.0


def alert_filter__xmatch_clu(
    database, alert, size_margin=3, clu_version="CLU_20190625"
) -> dict:
    """
        Filter to apply to each alert: cross-match with the CLU catalog

    :param database:
    :param alert:
    :param size_margin: multiply galaxy size by this much before looking for a match
    :param clu_version: CLU catalog version
    :return:
    """

    xmatches = dict()

    try:
        ra = float(alert["candidate"]["ra"])
        dec = float(alert["candidate"]["dec"])

        # geojson-friendly ra:
        ra_geojson = float(alert["candidate"]["ra"]) - 180.0
        dec_geojson = dec

        catalog_filter = {}
        catalog_projection = {
            "_id": 1,
            "name": 1,
            "ra": 1,
            "dec": 1,
            "a": 1,
            "b2a": 1,
            "pa": 1,
            "z": 1,
            "sfr_fuv": 1,
            "mstar": 1,
            "sfr_ha": 1,
            "coordinates.radec_str": 1,
        }

        # first do a coarse search of everything that is around
        object_position_query = dict()
        object_position_query["coordinates.radec_geojson"] = {
            "$geoWithin": {
                "$centerSphere": [[ra_geojson, dec_geojson], cone_search_radius_clu]
            }
        }
        galaxies = list(
            database[clu_version].find(
                {**object_position_query, **catalog_filter}, {**catalog_projection}
            )
        )

        # these guys are very big, so check them separately
        M31 = {
            "_id": 596900,
            "name": "PGC2557",
            "ra": 10.6847,
            "dec": 41.26901,
            "a": 6.35156,
            "b2a": 0.32,
            "pa": 35.0,
            "z": -0.00100100006,
            "sfr_fuv": None,
            "mstar": 253816876.412914,
            "sfr_ha": 0,
            "coordinates": {"radec_str": ["00:42:44.3503", "41:16:08.634"]},
        }
        M33 = {
            "_id": 597543,
            "name": "PGC5818",
            "ra": 23.46204,
            "dec": 30.66022,
            "a": 2.35983,
            "b2a": 0.59,
            "pa": 23.0,
            "z": -0.000597000006,
            "sfr_fuv": None,
            "mstar": 4502777.420493,
            "sfr_ha": 0,
            "coordinates": {"radec_str": ["01:33:50.8900", "30:39:36.800"]},
        }

        # do elliptical matches
        matches = []

        for galaxy in galaxies + [M31, M33]:
            alpha1, delta01 = galaxy["ra"], galaxy["dec"]
            d0, axis_ratio, PA0 = galaxy["a"], galaxy["b2a"], galaxy["pa"]

            # no shape info for galaxy? replace with median values
            if d0 < -990:
                d0 = 0.0265889
            if axis_ratio < -990:
                axis_ratio = 0.61
            if PA0 < -990:
                PA0 = 86.0

            in_galaxy = in_ellipse(
                ra, dec, alpha1, delta01, size_margin * d0, axis_ratio, PA0
            )

            if in_galaxy:
                match = galaxy
                distance_arcsec = round(
                    great_circle_distance(ra, dec, alpha1, delta01) * 3600, 2
                )
                match["coordinates"]["distance_arcsec"] = distance_arcsec
                matches.append(match)

        xmatches[clu_version] = matches

    except Exception as e:
        print(time_stamp(), str(e))

    return xmatches


def alert_filter__user_defined(
    database,
    filter_templates,
    alert,
    catalog: str = "ZTF_alerts",
    max_time_ms: int = 500,
) -> list:
    """
        Evaluate user-defined filters
    :param database:
    :param filter_templates:
    :param alert:
    :param catalog:
    :param max_time_ms:
    :return:
    """
    passed_filters = []

    for filter_template in filter_templates:
        try:
            _filter = deepcopy(filter_template)
            # match candid
            _filter["pipeline"][0]["$match"]["candid"] = alert["candid"]

            filtered_data = list(
                database[catalog].aggregate(
                    _filter["pipeline"], allowDiskUse=False, maxTimeMS=max_time_ms
                )
            )
            # passed filter? then len(passed_filter) must be = 1
            if len(filtered_data) == 1:
                log(
                    f'{alert["objectId"]} {alert["candid"]} passed filter {_filter["fid"]}'
                )
                passed_filters.append(
                    {
                        "group_id": _filter["group_id"],
                        "filter_id": _filter["filter_id"],
                        "group_name": _filter["group_name"],
                        "filter_name": _filter["filter_name"],
                        "fid": _filter["fid"],
                        "permissions": _filter["permissions"],
                        "data": filtered_data[0],
                    }
                )

        except Exception as e:
            print(
                f'{time_stamp()}: filter {filter_template["fid"]} execution failed on alert {alert["candid"]}: {e}'
            )
            continue

    return passed_filters


class AlertConsumer(object):
    """
    Creates an alert stream Kafka consumer for a given topic.

    Parameters
    ----------
    topic : `str`
        Name of the topic to subscribe to.
    schema_files : Avro schema files
        The reader Avro schema files for decoding data. Optional.
    **kwargs
        Keyword arguments for configuring confluent_kafka.Consumer().
    """

    def __init__(self, topic, **kwargs):

        self.verbose = kwargs.get("verbose", 2)

        # keep track of disconnected partitions
        self.num_disconnected_partitions = 0
        self.topic = topic

        def error_cb(err, _self=self):
            log(f"error_cb --------> {err}")
            # print(err.code())
            if err.code() == -195:
                _self.num_disconnected_partitions += 1
                if _self.num_disconnected_partitions == _self.num_partitions:
                    log("All partitions got disconnected, killing thread")
                    sys.exit()
                else:
                    log(
                        f"{_self.topic}: disconnected from partition. total: {_self.num_disconnected_partitions}"
                    )

        # 'error_cb': error_cb
        kwargs["error_cb"] = error_cb

        self.consumer = confluent_kafka.Consumer(**kwargs)
        self.num_partitions = 0

        def on_assign(consumer, partitions, _self=self):
            # force-reset offsets when subscribing to a topic:
            for part in partitions:
                # -2 stands for beginning and -1 for end
                part.offset = -2
                # keep number of partitions.
                # when reaching end of last partition, kill thread and start from beginning
                _self.num_partitions += 1
                log(consumer.get_watermark_offsets(part))

        self.consumer.subscribe([topic], on_assign=on_assign)

        self.config = config

        # MongoDB collections to store the alerts:
        self.collection_alerts = self.config["database"]["collections"]["alerts_ztf"]
        self.collection_alerts_aux = self.config["database"]["collections"][
            "alerts_ztf_aux"
        ]
        self.collection_alerts_filter = self.config["database"]["collections"][
            "alerts_ztf_filter"
        ]

        self.mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=self.verbose,
        )

        # create indexes
        if self.config["database"]["build_indexes"]:
            for index_name, index in self.config["database"]["indexes"][
                self.collection_alerts
            ].items():
                ind = [tuple(ii) for ii in index]
                self.mongo.db[self.collection_alerts].create_index(
                    keys=ind, name=index_name, background=True
                )

        # ML models:
        self.ml_models = dict()
        for model in config["ml_models"]:
            try:
                model_version = config["ml_models"][model]["version"]
                # todo: allow other formats such as SavedModel
                model_filepath = os.path.join(
                    config["path"]["ml_models"], f"{model}_{model_version}.h5"
                )
                self.ml_models[model] = {
                    "model": load_model(model_filepath),
                    "version": model_version,
                }
            except Exception as e:
                log(f"Error loading ML model {model}: {str(e)}")
                _err = traceback.format_exc()
                log(_err)
                continue

        # talking to SkyPortal?
        if config["misc"]["broker"]:
            # session to talk to SkyPortal
            self.session = requests.Session()
            self.session_headers = {
                "Authorization": f"token {config['skyportal']['token']}"
            }

            retries = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[405, 429, 500, 502, 503, 504],
                method_whitelist=["HEAD", "GET", "PUT", "POST", "PATCH"],
            )
            adapter = TimeoutHTTPAdapter(timeout=5, max_retries=retries)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)

            # get ZTF instrument id
            self.instrument_id = 1
            try:
                tic = time.time()
                response = self.api_skyportal("GET", "/api/instrument", {"name": "ZTF"})
                toc = time.time()
                if self.verbose > 1:
                    log(f"Getting ZTF instrument_id from SkyPortal took {toc - tic} s")
                if (
                    response.json()["status"] == "success"
                    and len(response.json()["data"]) > 0
                ):
                    self.instrument_id = response.json()["data"][0]["id"]
                    log(f"Got ZTF instrument_id from SkyPortal: {self.instrument_id}")
                else:
                    log("Failed to get ZTF instrument_id from SkyPortal")
                    raise ValueError("Failed to get ZTF instrument_id from SkyPortal")
            except Exception as e:
                log(e)
                # config['misc']['broker'] = False

            # filter pipeline upstream: select current alert, ditch cutouts, and merge with aux data
            # including archival photometry and cross-matches:
            self.filter_pipeline_upstream = config["database"]["filters"][
                self.collection_alerts
            ]
            log("Upstream filtering pipeline:")
            log(self.filter_pipeline_upstream)

            # load *active* user-defined alert filter templates
            active_filters = list(
                self.mongo.db[config["database"]["collections"]["filters"]].aggregate(
                    [
                        {"$match": {"catalog": "ZTF_alerts", "active": True}},
                        {
                            "$project": {
                                "group_id": 1,
                                "filter_id": 1,
                                "permissions": 1,
                                "fv": {
                                    "$arrayElemAt": [
                                        {
                                            "$filter": {
                                                "input": "$fv",
                                                "as": "fvv",
                                                "cond": {
                                                    "$eq": ["$$fvv.fid", "$active_fid"]
                                                },
                                            }
                                        },
                                        0,
                                    ]
                                },
                            }
                        },
                    ]
                )
            )

            # todo: query SP to make sure the filters still exist there and we're not out of sync;
            #       clean up if necessary

            self.filter_templates = []
            for active_filter in active_filters:
                # collect additional info from SkyPortal
                tic = time.time()
                response = self.api_skyportal(
                    "GET", f"/api/groups/{active_filter['group_id']}"
                )
                toc = time.time()
                if self.verbose > 1:
                    log(
                        f"Getting info on group id={active_filter['group_id']} from SkyPortal took {toc - tic} s"
                    )
                    log(response.json())
                if response.json()["status"] == "success":
                    group_name = (
                        response.json()["data"]["nickname"]
                        if response.json()["data"]["nickname"] is not None
                        else response.json()["data"]["name"]
                    )
                    filter_name = [
                        f["name"]
                        for f in response.json()["data"]["filters"]
                        if f["id"] == active_filter["filter_id"]
                    ][0]
                else:
                    log(
                        f"Failed to get info on group id={active_filter['group_id']} from SkyPortal"
                    )
                    group_name, filter_name = None, None
                    # raise ValueError(f"Failed to get info on group id={active_filter['group_id']} from SkyPortal")
                log(f"Group name: {group_name}, filter name: {filter_name}")

                # prepend upstream aggregation stages:
                pipeline = deepcopy(self.filter_pipeline_upstream) + loads(
                    active_filter["fv"]["pipeline"]
                )
                # match permissions
                pipeline[0]["$match"]["candidate.programid"]["$in"] = active_filter[
                    "permissions"
                ]
                pipeline[3]["$project"]["prv_candidates"]["$filter"]["cond"]["$and"][0][
                    "$in"
                ][1] = active_filter["permissions"]

                filter_template = {
                    "group_id": active_filter["group_id"],
                    "filter_id": active_filter["filter_id"],
                    "group_name": group_name,
                    "filter_name": filter_name,
                    "fid": active_filter["fv"]["fid"],
                    "permissions": active_filter["permissions"],
                    "pipeline": deepcopy(pipeline),
                }

                self.filter_templates.append(filter_template)

            log("Science filters:")
            log(self.filter_templates)

    def api_skyportal(self, method: str, endpoint: str, data=None):
        """Make an API call to a SkyPortal instance

        :param method:
        :param endpoint:
        :param data:
        :return:
        """
        method = method.lower()
        methods = {
            "head": self.session.head,
            "get": self.session.get,
            "post": self.session.post,
            "put": self.session.put,
            "patch": self.session.patch,
            "delete": self.session.delete,
        }

        if endpoint is None:
            raise ValueError("Endpoint not specified")
        if method not in ["head", "get", "post", "put", "patch", "delete"]:
            raise ValueError(f"Unsupported method: {method}")

        if method == "get":
            response = methods[method](
                f"{config['skyportal']['protocol']}://"
                f"{config['skyportal']['host']}:{config['skyportal']['port']}"
                f"{endpoint}",
                params=data,
                headers=self.session_headers,
            )
        else:
            response = methods[method.lower()](
                f"{config['skyportal']['protocol']}://"
                f"{config['skyportal']['host']}:{config['skyportal']['port']}"
                f"{endpoint}",
                json=data,
                headers=self.session_headers,
            )

        return response

    @staticmethod
    def alert_mongify(alert):

        doc = dict(alert)

        # let mongo create a unique _id

        # placeholders for classifications
        doc["classifications"] = dict()

        # GeoJSON for 2D indexing
        doc["coordinates"] = {}
        _ra = doc["candidate"]["ra"]
        _dec = doc["candidate"]["dec"]
        # string format: H:M:S, D:M:S
        _radec_str = [deg2hms(_ra), deg2dms(_dec)]
        doc["coordinates"]["radec_str"] = _radec_str
        # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
        _radec_geojson = [_ra - 180.0, _dec]
        doc["coordinates"]["radec_geojson"] = {
            "type": "Point",
            "coordinates": _radec_geojson,
        }

        # Galactic coordinates l and b
        l, b = radec2lb(doc["candidate"]["ra"], doc["candidate"]["dec"])
        doc["coordinates"]["l"] = l
        doc["coordinates"]["b"] = b

        prv_candidates = deepcopy(doc["prv_candidates"])
        doc.pop("prv_candidates", None)
        if prv_candidates is None:
            prv_candidates = []

        return doc, prv_candidates

    def alert_post_candidate(self, alert, filter_ids):
        # post metadata with all filter_ids in single call to /api/candidates
        alert_thin = {
            "id": alert["objectId"],
            "ra": alert["candidate"].get("ra"),
            "dec": alert["candidate"].get("dec"),
            "score": alert["candidate"].get("drb", alert["candidate"]["rb"]),
            "filter_ids": filter_ids,
            "passing_alert_id": alert["candid"],
            "passed_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        }
        if self.verbose > 1:
            log(alert_thin)

        tic = time.time()
        response = self.api_skyportal("POST", "/api/candidates", alert_thin)
        toc = time.time()
        if self.verbose > 1:
            log(f"Posting metadata to SkyPortal took {toc - tic} s")
        if response.json()["status"] == "success":
            log(f"Posted {alert['objectId']} {alert['candid']} metadata to SkyPortal")
        else:
            log(
                f"Failed to post {alert['objectId']} {alert['candid']} metadata to SkyPortal"
            )
            log(response.json())

    def alert_put_candidate(self, alert, filter_ids):
        # update candidate metadata for (existing) filter_ids
        alert_thin = {
            "filter_ids": filter_ids,
            "passing_alert_id": alert["candid"],
            "passed_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f"),
        }
        if self.verbose > 1:
            log(alert_thin)

        tic = time.time()
        response = self.api_skyportal(
            "PUT", f"/api/candidates/{alert['objectId']}", alert_thin
        )
        toc = time.time()
        if self.verbose > 1:
            log(f"Putting metadata to SkyPortal took {toc - tic} s")
        if response.json()["status"] == "success":
            log(f"Put {alert['objectId']} {alert['candid']} metadata to SkyPortal")
        else:
            log(
                f"Failed to put {alert['objectId']} {alert['candid']} metadata to SkyPortal"
            )
            log(response.json())

    def alert_post_annotations(self, alert, passed_filters):
        for passed_filter in passed_filters:
            annotations = {
                "obj_id": alert["objectId"],
                "origin": f"{passed_filter.get('group_name')}:{passed_filter.get('filter_name')}",
                "data": passed_filter.get("data", dict()).get("annotations", dict()),
                "group_ids": [passed_filter.get("group_id")],
            }
            tic = time.time()
            response = self.api_skyportal("POST", "/api/annotation", annotations)
            toc = time.time()
            if self.verbose > 1:
                log(
                    f"Posting annotation for {alert['objectId']} to skyportal took {toc - tic} s"
                )
            if response.json()["status"] == "success":
                log(f"Posted {alert['objectId']} annotation to SkyPortal")
            else:
                log(f"Failed to post {alert['objectId']} annotation to SkyPortal")
                log(response.json())

    def alert_post_thumbnails(self, alert):
        for ttype, ztftype in [
            ("new", "Science"),
            ("ref", "Template"),
            ("sub", "Difference"),
        ]:
            tic = time.time()
            thumb = make_thumbnail(alert, ttype, ztftype)
            toc = time.time()
            if self.verbose > 1:
                log(f"Making {ztftype} thumbnail took {toc - tic} s")

            tic = time.time()
            response = self.api_skyportal("POST", "/api/thumbnail", thumb)
            toc = time.time()
            if self.verbose > 1:
                log(f"Posting {ztftype} thumbnail to SkyPortal took {toc - tic} s")

            if response.json()["status"] == "success":
                log(
                    f"Posted {alert['objectId']} {alert['candid']} {ztftype} cutout to SkyPortal"
                )
            else:
                log(
                    f"Failed to post {alert['objectId']} {alert['candid']} {ztftype} cutout to SkyPortal"
                )
                log(response.json())

    def alert_put_photometry(self, alert, groups):
        """PUT photometry to SkyPortal

        :param alert:
        :param groups: array of dicts containing at least
                       [{"group_id": <group_id>, "permissions": <list of program ids the group has access to>}]
        :return:
        """
        tic = time.time()
        df_photometry = make_photometry(alert)
        toc = time.time()
        if self.verbose > 1:
            log(f"Making alert photometry took {toc - tic} s")

        # post data from different program_id's
        for pid in set(df_photometry.programid.unique()):
            group_ids = [
                f.get("group_id") for f in groups if pid in f.get("permissions", [1])
            ]

            if len(group_ids) > 0:
                pid_mask = df_photometry.programid == int(pid)

                photometry = {
                    "obj_id": alert["objectId"],
                    "group_ids": group_ids,
                    "instrument_id": self.instrument_id,
                    "mjd": df_photometry.loc[pid_mask, "mjd"].tolist(),
                    "mag": df_photometry.loc[pid_mask, "magpsf"].tolist(),
                    "magerr": df_photometry.loc[pid_mask, "sigmapsf"].tolist(),
                    "limiting_mag": df_photometry.loc[pid_mask, "diffmaglim"].tolist(),
                    "magsys": df_photometry.loc[pid_mask, "magsys"].tolist(),
                    "filter": df_photometry.loc[pid_mask, "ztf_filter"].tolist(),
                    "ra": df_photometry.loc[pid_mask, "ra"].tolist(),
                    "dec": df_photometry.loc[pid_mask, "dec"].tolist(),
                }

                if len(photometry.get("mag", ())) > 0:
                    tic = time.time()
                    response = self.api_skyportal("PUT", "/api/photometry", photometry)
                    toc = time.time()
                    if self.verbose > 1:
                        log(
                            f"Posting photometry of {alert['objectId']} {alert['candid']}, "
                            f"program_id={pid} to SkyPortal took {toc - tic} s"
                        )
                    if response.json()["status"] == "success":
                        log(
                            f"Posted {alert['objectId']} program_id={pid} photometry to SkyPortal"
                        )
                    else:
                        log(
                            f"Failed to post {alert['objectId']} program_id={pid} photometry to SkyPortal"
                        )
                    log(response.json())

    def alert_sentinel_skyportal(self, alert, prv_candidates, passed_filters):
        """
        Post alerts to SkyPortal, if need be.

        Logic:
        - check if candidate/source exist on SP
        - if candidate does not exist:
          - if len(passed_filters) > 0
            - post metadata with all filter_ids in single call to /api/candidates
            - post full light curve with all group_ids in single call to /api/photometry
            - post thumbnails
        - if candidate exists:
          - get filter_ids of saved candidate from SP
          - post to /api/candidates with new_filter_ids, if any
          - post alert curve with group_ids of corresponding filters in single call to /api/photometry
        - if source exists:
          - get groups and check stream access
          - decide which points to post to what groups based on permissions
          - post alert curve with all group_ids in single call to /api/photometry

        :param alert: ZTF_alert with a stripped-off prv_candidates section
        :param prv_candidates: could be plain prv_candidates section of an alert, or extended alert history
        :param passed_filters: list of filters that alert passed, with their output
        :return:
        """
        # check if candidate/source exist in SP:
        tic = time.time()
        response = self.api_skyportal("HEAD", f"/api/candidates/{alert['objectId']}")
        toc = time.time()
        is_candidate = response.status_code == 200
        if self.verbose > 1:
            log(f"Checking if object is Candidate took {toc - tic} s")
            log(
                f"{alert['objectId']} {'is' if is_candidate else 'is not'} Candidate in SkyPortal"
            )
        tic = time.time()
        response = self.api_skyportal("HEAD", f"/api/sources/{alert['objectId']}")
        toc = time.time()
        is_source = response.status_code == 200
        if self.verbose > 1:
            log(f"Checking if object is Source took {toc - tic} s")
            log(
                f"{alert['objectId']} {'is' if is_source else 'is not'} Source in SkyPortal"
            )

        # obj does not exit in SP:
        if (not is_candidate) and (not is_source):
            # passed any filters?
            if len(passed_filters) > 0:
                # post candidate
                filter_ids = [f.get("filter_id") for f in passed_filters]
                self.alert_post_candidate(alert, filter_ids)

                # post annotations
                self.alert_post_annotations(alert, passed_filters)

                # post full light curve
                try:
                    alert["prv_candidates"] = list(
                        self.mongo.db[self.collection_alerts_aux].find(
                            {"_id": alert["objectId"]}, {"prv_candidates": 1}, limit=1
                        )
                    )[0]["prv_candidates"]
                except Exception as e:
                    # this should never happen, but just in case
                    log(e)
                    alert["prv_candidates"] = prv_candidates

                self.alert_put_photometry(alert, passed_filters)

                # post thumbnails
                self.alert_post_thumbnails(alert)

        # obj exists in SP:
        else:
            if len(passed_filters) > 0:
                filter_ids = [f.get("filter_id") for f in passed_filters]
                # already posted as a candidate?
                if is_candidate:
                    # get filter_ids of saved candidate from SP
                    tic = time.time()
                    response = self.api_skyportal(
                        "GET", f"/api/candidates/{alert['objectId']}"
                    )
                    toc = time.time()
                    if self.verbose > 1:
                        log(
                            f"Getting candidate info on {alert['objectId']} took {toc - tic} s"
                        )
                    if response.json()["status"] == "success":
                        existing_filter_ids = response.json()["data"]["filter_ids"]
                        # do not post to existing_filter_ids
                        filter_ids = list(set(filter_ids) - set(existing_filter_ids))
                        # update existing candidate info
                        self.alert_put_candidate(alert, existing_filter_ids)
                    else:
                        log(f"Failed to get candidate info on {alert['objectId']}")

                if len(filter_ids) > 0:
                    # post candidate with new filter ids
                    self.alert_post_candidate(alert, filter_ids)

                    # post annotations
                    self.alert_post_annotations(
                        alert,
                        [
                            pf
                            for pf in passed_filters
                            if pf.get("filter_id") in filter_ids
                        ],
                    )

            groups = passed_filters
            group_ids = [f.get("group_id") for f in passed_filters]
            # already saved as a source?
            if is_source:
                # get info on the corresponding groups:
                tic = time.time()
                response = self.api_skyportal(
                    "GET", f"/api/sources/{alert['objectId']}"
                )
                toc = time.time()
                if self.verbose > 1:
                    log(
                        f"Getting source info on {alert['objectId']} took {toc - tic} s"
                    )
                if response.json()["status"] == "success":
                    existing_group_ids = [
                        g["id"] for g in response.json()["data"]["groups"]
                    ]
                    for group_id in set(existing_group_ids) - set(group_ids):
                        tic = time.time()
                        response = self.api_skyportal("GET", f"/api/groups/{group_id}")
                        toc = time.time()
                        if self.verbose > 1:
                            log(
                                f"Getting info on group id={group_id} from SkyPortal took {toc - tic} s"
                            )
                            log(response.json())
                        if response.json()["status"] == "success":
                            selector = {1}
                            for stream in response.json()["data"]["streams"]:
                                if "ztf" in stream["name"].lower():
                                    selector.update(
                                        set(stream["altdata"].get("selector", []))
                                    )

                            selector = list(selector)

                            groups.append(
                                {"group_id": group_id, "permissions": selector}
                            )
                        else:
                            log(
                                f"Failed to get info on group id={group_id} from SkyPortal"
                            )
                else:
                    log(f"Failed to get source info on {alert['objectId']}")

            # post alert photometry with all group_ids in single call to /api/photometry
            alert["prv_candidates"] = prv_candidates

            self.alert_put_photometry(alert, groups)

    def poll(self, verbose=2):
        """
        Polls Kafka broker to consume topic.

        :param verbose: 1 - main, 2 - debug
        :return:
        """
        msg = self.consumer.poll()

        if msg is None:
            print(time_stamp(), "Caught error: msg is None")

        if msg.error():
            print(time_stamp(), "Caught error:", msg.error())
            raise EopError(msg)

        elif msg is not None:
            try:
                # decode avro packet
                msg_decoded = self.decode_message(msg)
                for record in msg_decoded:

                    candid = record["candid"]
                    objectId = record["objectId"]

                    log(f"{self.topic} {objectId} {candid}")

                    # check that candid not in collection_alerts
                    if (
                        self.mongo.db[self.collection_alerts].count_documents(
                            {"candid": candid}, limit=1
                        )
                        == 0
                    ):
                        # candid not in db, ingest decoded avro packet into db
                        # todo: ?? restructure alerts even further?
                        #       move cutouts to ZTF_alerts_cutouts? reduce the main db size for performance
                        #       group by objectId similar to prv_candidates?? maybe this is too much
                        tic = time.time()
                        alert, prv_candidates = self.alert_mongify(record)
                        toc = time.time()
                        if verbose > 1:
                            print(f"{time_stamp()}: mongification took {toc - tic} s")

                        # ML models:
                        tic = time.time()
                        scores = alert_filter__ml(record, ml_models=self.ml_models)
                        alert["classifications"] = scores
                        toc = time.time()
                        if verbose > 1:
                            print(f"{time_stamp()}: mling took {toc - tic} s")

                        print(f'{time_stamp()}: ingesting {alert["candid"]} into db')
                        tic = time.time()
                        self.mongo.insert_one(
                            collection=self.collection_alerts, document=alert
                        )
                        toc = time.time()
                        if verbose > 1:
                            print(
                                f'{time_stamp()}: ingesting {alert["candid"]} took {toc - tic} s'
                            )

                        # prv_candidates: pop nulls - save space
                        prv_candidates = [
                            {
                                kk: vv
                                for kk, vv in prv_candidate.items()
                                if vv is not None
                            }
                            for prv_candidate in prv_candidates
                        ]

                        # cross-match with external catalogs if objectId not in collection_alerts_aux:
                        if (
                            self.mongo.db[self.collection_alerts_aux].count_documents(
                                {"_id": objectId}, limit=1
                            )
                            == 0
                        ):
                            tic = time.time()
                            xmatches = alert_filter__xmatch(self.mongo.db, alert)
                            toc = time.time()
                            if verbose > 1:
                                log(f"Xmatch took {toc - tic} s")
                            # CLU cross-match:
                            tic = time.time()
                            xmatches = {
                                **xmatches,
                                **alert_filter__xmatch_clu(self.mongo.db, alert),
                            }
                            toc = time.time()
                            if verbose > 1:
                                log(f"CLU xmatch took {toc - tic} s")

                            alert_aux = {
                                "_id": objectId,
                                "cross_matches": xmatches,
                                "prv_candidates": prv_candidates,
                            }

                            tic = time.time()
                            self.mongo.insert_one(
                                collection=self.collection_alerts_aux,
                                document=alert_aux,
                            )
                            toc = time.time()
                            if verbose > 1:
                                print(
                                    f"{time_stamp()}: aux ingesting took {toc - tic} s"
                                )

                        else:
                            tic = time.time()
                            self.mongo.db[self.collection_alerts_aux].update_one(
                                {"_id": objectId},
                                {
                                    "$addToSet": {
                                        "prv_candidates": {"$each": prv_candidates}
                                    }
                                },
                                upsert=True,
                            )
                            toc = time.time()
                            if verbose > 1:
                                print(
                                    f"{time_stamp()}: aux updating took {toc - tic} s"
                                )

                        if config["misc"]["broker"]:
                            # execute user-defined alert filters
                            tic = time.time()
                            passed_filters = alert_filter__user_defined(
                                self.mongo.db, self.filter_templates, alert
                            )
                            toc = time.time()
                            if verbose > 1:
                                log(f"Filtering took {toc-tic} s")

                            # post to SkyPortal
                            self.alert_sentinel_skyportal(
                                alert, prv_candidates, passed_filters
                            )

            except Exception as e:
                log(e)
                _err = traceback.format_exc()
                log(_err)

    @staticmethod
    def decode_message(msg):
        """Decode Avro message according to a schema.

        Parameters
        ----------
        msg : Kafka message
            The Kafka message result from consumer.poll().

        Returns
        -------
        `dict`
            Decoded message.
        """
        # print(msg.topic(), msg.offset(), msg.error(), msg.key(), msg.value())
        message = msg.value()
        decoded_msg = message

        try:
            bytes_io = io.BytesIO(message)
            decoded_msg = read_schema_data(bytes_io)
        except AssertionError:
            decoded_msg = None
        except IndexError:
            literal_msg = literal_eval(
                str(message, encoding="utf-8")
            )  # works to give bytes
            bytes_io = io.BytesIO(literal_msg)  # works to give <class '_io.BytesIO'>
            decoded_msg = read_schema_data(bytes_io)  # yields reader
        except Exception:
            decoded_msg = message
        finally:
            return decoded_msg


def listener(
    topic, bootstrap_servers="", offset_reset="earliest", group=None, test=False
):
    """
        Listen to a topic
    :param topic:
    :param bootstrap_servers:
    :param offset_reset:
    :param group:
    :param test: when testing, terminate once reached end of partition
    :return:
    """

    # Configure consumer connection to Kafka broker
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "default.topic.config": {"auto.offset.reset": offset_reset},
    }
    if group is not None:
        conf["group.id"] = group
    else:
        conf["group.id"] = os.environ.get("HOSTNAME", "kowalski")

    # make it unique:
    conf[
        "group.id"
    ] = f"{conf['group.id']}_{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')}"

    # Start alert stream consumer
    stream_reader = AlertConsumer(topic, **conf)

    while True:
        try:
            # poll!
            stream_reader.poll()

        except EopError as e:
            # Write when reaching end of partition
            print(f"{time_stamp()}: {e.message}")
            if test:
                # when testing, terminate once reached end of partition:
                sys.exit()
        except IndexError:
            print(time_stamp(), "%% Data cannot be decoded\n")
        except UnicodeDecodeError:
            print(time_stamp(), "%% Unexpected data format received\n")
        except KeyboardInterrupt:
            print(time_stamp(), "%% Aborted by user\n")
            sys.exit()
        except Exception as e:
            print(time_stamp(), str(e))
            _err = traceback.format_exc()
            print(time_stamp(), str(_err))
            sys.exit()


def ingester(obs_date=None, test=False):
    """
        Watchdog for topic listeners

    :param obs_date: observing date: YYYYMMDD
    :param test: test mode
    :return:
    """

    topics_on_watch = dict()

    while True:

        try:
            # get kafka topic names with kafka-topics command
            if not test:
                # Production Kafka stream at IPAC
                kafka_cmd = [
                    os.path.join(config["path"]["kafka"], "bin", "kafka-topics.sh"),
                    "--zookeeper",
                    config["kafka"]["zookeeper"],
                    "-list",
                ]
            else:
                # Local test stream
                kafka_cmd = [
                    os.path.join(config["path"]["kafka"], "bin", "kafka-topics.sh"),
                    "--zookeeper",
                    config["kafka"]["zookeeper.test"],
                    "-list",
                ]

            topics = (
                subprocess.run(kafka_cmd, stdout=subprocess.PIPE)
                .stdout.decode("utf-8")
                .split("\n")[:-1]
            )

            if obs_date is None:
                datestr = datetime.datetime.utcnow().strftime("%Y%m%d")
            else:
                datestr = obs_date
            # as of 20180403, the naming convention is ztf_%Y%m%d_programidN
            # exclude ZUDS, ingest separately
            topics_tonight = [
                t
                for t in topics
                if (datestr in t) and ("programid" in t) and ("zuds" not in t)
            ]
            print(f"{time_stamp()}: Topics: {topics_tonight}")

            for t in topics_tonight:
                if t not in topics_on_watch:
                    print(f"{time_stamp()}: starting listener thread for {t}")
                    offset_reset = config["kafka"]["default.topic.config"][
                        "auto.offset.reset"
                    ]
                    if not test:
                        bootstrap_servers = config["kafka"]["bootstrap.servers"]
                    else:
                        bootstrap_servers = config["kafka"]["bootstrap.test.servers"]
                    group = config["kafka"]["group"]

                    topics_on_watch[t] = multiprocessing.Process(
                        target=listener,
                        args=(t, bootstrap_servers, offset_reset, group, test),
                    )
                    topics_on_watch[t].daemon = True
                    topics_on_watch[t].start()

                else:
                    print(f"{time_stamp()}: performing thread health check for {t}")
                    try:
                        if not topics_on_watch[t].is_alive():
                            print(f"{time_stamp()}: {t} died, removing")
                            # topics_on_watch[t].terminate()
                            topics_on_watch.pop(t, None)
                        else:
                            print(f"{time_stamp()}: {t} appears normal")
                    except Exception as _e:
                        print(
                            f"{time_stamp()}: Failed to perform health check", str(_e)
                        )
                        pass

            if test:
                time.sleep(240)
                # when testing, wait for topic listeners to pull all the data, then break
                for t in topics_on_watch:
                    topics_on_watch[t].kill()
                break

        except Exception as e:
            log(str(e))
            _err = traceback.format_exc()
            log(str(_err))

        if obs_date is None:
            time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch AVRO packets from Kafka streams and ingest them into DB"
    )
    parser.add_argument("--obsdate", help="observing date YYYYMMDD")
    parser.add_argument("--test", help="listen to the test stream", action="store_true")

    args = parser.parse_args()

    ingester(obs_date=args.obsdate, test=args.test)
