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
import dask.distributed
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
from requests.packages.urllib3.util.retry import Retry
import subprocess
import sys
import tensorflow as tf
from tensorflow.keras.models import load_model
import threading
import time
import traceback
from typing import Mapping, Optional, Sequence

from utils import (
    deg2dms,
    deg2hms,
    great_circle_distance,
    in_ellipse,
    init_db_sync,
    load_config,
    log,
    memoize,
    Mongo,
    radec2lb,
    time_stamp,
    timer,
    TimeoutHTTPAdapter,
    ZTFAlert,
)


tf.config.optimizer.set_jit(True)


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


def read_schema_data(bytes_io):
    """
    Read data that already has an Avro schema.

    :param bytes_io: `_io.BytesIO` Data to be decoded.
    :return: `dict` Decoded data.
    """
    bytes_io.seek(0)
    message = fastavro.reader(bytes_io)
    return message


class EopError(Exception):
    """
    Exception raised when reaching end of a Kafka topic partition.
    """

    def __init__(self, msg):
        """
        :param msg: The Kafka message result from consumer.poll()
        """
        message = (
            f"{time_stamp()}: topic:{msg.topic()}, partition:{msg.partition()}, "
            f"status:end, offset:{msg.offset()}, key:{str(msg.key())}\n"
        )
        self.message = message

    def __str__(self):
        return self.message


def make_photometry(alert: dict, jd_start: float = None):
    """
    Make a de-duplicated pandas.DataFrame with photometry of alert['objectId']

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

    # convert from mag to flux

    # step 1: calculate the coefficient that determines whether the
    # flux should be negative or positive
    coeff = df_light_curve["isdiffpos"].apply(
        lambda x: 1.0 if x in [True, 1, "y", "Y"] else -1.0
    )

    # step 2: calculate the flux normalized to an arbitrary AB zeropoint of
    # 23.9 (results in flux in uJy)
    df_light_curve["flux"] = coeff * 10 ** (-0.4 * (df_light_curve["magpsf"] - 23.9))

    # step 3: separate detections from non detections
    detected = np.isfinite(df_light_curve["magpsf"])
    undetected = ~detected

    # step 4: calculate the flux error
    df_light_curve["fluxerr"] = None  # initialize the column

    # step 4a: calculate fluxerr for detections using sigmapsf
    df_light_curve.loc[detected, "fluxerr"] = np.abs(
        df_light_curve.loc[detected, "sigmapsf"]
        * df_light_curve.loc[detected, "flux"]
        * np.log(10)
        / 2.5
    )

    # step 4b: calculate fluxerr for non detections using diffmaglim
    df_light_curve.loc[undetected, "fluxerr"] = (
        10 ** (-0.4 * (df_light_curve.loc[undetected, "diffmaglim"] - 23.9)) / 5.0
    )  # as diffmaglim is the 5-sigma depth

    # step 5: set the zeropoint and magnitude system
    df_light_curve["zp"] = 23.9
    df_light_curve["zpsys"] = "ab"

    # only "new" photometry requested?
    if jd_start is not None:
        w_after_jd = df_light_curve["jd"] > jd_start
        df_light_curve = df_light_curve.loc[w_after_jd]

    return df_light_curve


def make_thumbnail(alert, ttype: str, ztftype: str):
    """
    Convert lossless FITS cutouts from ZTF alerts into PNGs

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

    # replace nans with median:
    img = np.array(data_flipped_y)
    # replace dubiously large values
    xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
    if img[xl].any():
        img[xl] = np.nan
    if np.isnan(img).any():
        median = float(np.nanmean(img.flatten()))
        img = np.nan_to_num(img, nan=median)

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
    """
    Make an L2-normalized cutout triplet out of a ZTF alert

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
    """Execute ML models on a ZTF alert

    :param alert:
    :param ml_models:
    :return:
    """

    scores = dict()

    if ml_models is not None and len(ml_models) > 0:
        try:
            with timer("ZTFAlert(alert)"):
                ztf_alert = ZTFAlert(alert)
            with timer("Prepping features"):
                features = np.expand_dims(ztf_alert.data["features"], axis=[0, -1])
                triplet = np.expand_dims(ztf_alert.data["triplet"], axis=[0])

            # braai
            if "braai" in ml_models.keys():
                with timer("braai"):
                    braai = ml_models["braai"]["model"].predict(x=triplet)[0]
                    scores["braai"] = float(braai)
                    scores["braai_version"] = ml_models["braai"]["version"]
            # acai
            for model_name in ("acai_h", "acai_v", "acai_o", "acai_n", "acai_b"):
                if model_name in ml_models.keys():
                    with timer(model_name):
                        score = ml_models[model_name]["model"].predict(
                            [features, triplet]
                        )[0]
                        scores[model_name] = float(score)
                        scores[f"{model_name}_version"] = ml_models[model_name][
                            "version"
                        ]
        except Exception as e:
            log(str(e))

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
        log(str(e))

    return xmatches


# cone search radius in deg:
cone_search_radius_clu = 3.0
# convert deg to rad:
cone_search_radius_clu *= np.pi / 180.0


def alert_filter__xmatch_clu(
    database, alert, size_margin=3, clu_version="CLU_20190625"
) -> dict:
    """
    Run cross-match with the CLU catalog

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

            redshift = galaxy["z"]
            # By default, set the cross-match radius to 50 kpc at the redshift of the host galaxy
            cm_radius = 50.0 * (0.05 / redshift) / 3600
            if redshift < 0.01:
                # for nearby galaxies and galaxies with negative redshifts, do a 5 arc-minute cross-match
                # (cross-match radius would otherwise get un-physically large for nearby galaxies)
                cm_radius = 300.0 / 3600

            in_galaxy = in_ellipse(ra, dec, alpha1, delta01, cm_radius, 1, 0)

            if in_galaxy:
                match = galaxy
                distance_arcsec = round(
                    great_circle_distance(ra, dec, alpha1, delta01) * 3600, 2
                )
                # also add a physical distance parameter for redshifts in the Hubble flow
                if redshift > 0.005:
                    distance_kpc = round(
                        great_circle_distance(ra, dec, alpha1, delta01)
                        * 3600
                        * (redshift / 0.05),
                        2,
                    )
                else:
                    distance_kpc = -1

                match["coordinates"]["distance_arcsec"] = distance_arcsec
                match["coordinates"]["distance_kpc"] = distance_kpc
                matches.append(match)

        xmatches[clu_version] = matches

    except Exception as e:
        log(str(e))

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
                        "autosave": _filter["autosave"],
                        "update_annotations": _filter["update_annotations"],
                        "data": filtered_data[0],
                    }
                )

        except Exception as e:
            log(
                f'Filter {filter_template["fid"]} execution failed on alert {alert["candid"]}: {e}'
            )
            continue

    return passed_filters


def process_alert(record, topic):
    """Alert brokering task run by dask.distributed workers

    :param record: decoded alert from IPAC's Kafka stream
    :param topic: Kafka stream topic name for bookkeeping
    :return:
    """
    candid = record["candid"]
    objectId = record["objectId"]

    # get worker running current task
    worker = dask.distributed.get_worker()
    alert_worker = worker.plugins["worker-init"].alert_worker

    log(f"{topic} {objectId} {candid} {worker.address}")

    # return if this alert packet has already been processed and ingested into collection_alerts:
    if (
        alert_worker.mongo.db[alert_worker.collection_alerts].count_documents(
            {"candid": candid}, limit=1
        )
        == 1
    ):
        return

    # candid not in db, ingest decoded avro packet into db
    # todo: ?? restructure alerts even further?
    #       move cutouts to ZTF_alerts_cutouts? reduce the main db size for performance
    #       group by objectId similar to prv_candidates?? maybe this is too much
    with timer(f"Mongification of {objectId} {candid}", alert_worker.verbose > 1):
        alert, prv_candidates = alert_worker.alert_mongify(record)

    # ML models:
    with timer(f"MLing of {objectId} {candid}", alert_worker.verbose > 1):
        scores = alert_filter__ml(record, ml_models=alert_worker.ml_models)
        alert["classifications"] = scores

    with timer(f"Ingesting {objectId} {candid}", alert_worker.verbose > 1):
        alert_worker.mongo.insert_one(
            collection=alert_worker.collection_alerts, document=alert
        )

    # prv_candidates: pop nulls - save space
    prv_candidates = [
        {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
        for prv_candidate in prv_candidates
    ]

    # cross-match with external catalogs if objectId not in collection_alerts_aux:
    if (
        alert_worker.mongo.db[alert_worker.collection_alerts_aux].count_documents(
            {"_id": objectId}, limit=1
        )
        == 0
    ):
        with timer(f"Cross-match of {objectId} {candid}", alert_worker.verbose > 1):
            xmatches = alert_filter__xmatch(alert_worker.mongo.db, alert)
        # CLU cross-match:
        with timer(f"CLU cross-match {objectId} {candid}", alert_worker.verbose > 1):
            xmatches = {
                **xmatches,
                **alert_filter__xmatch_clu(alert_worker.mongo.db, alert),
            }

        alert_aux = {
            "_id": objectId,
            "cross_matches": xmatches,
            "prv_candidates": prv_candidates,
        }

        with timer(f"Aux ingesting {objectId} {candid}", alert_worker.verbose > 1):
            alert_worker.mongo.insert_one(
                collection=alert_worker.collection_alerts_aux, document=alert_aux
            )

    else:
        with timer(f"Aux updating of {objectId} {candid}", alert_worker.verbose > 1):
            alert_worker.mongo.db[alert_worker.collection_alerts_aux].update_one(
                {"_id": objectId},
                {"$addToSet": {"prv_candidates": {"$each": prv_candidates}}},
                upsert=True,
            )

    if config["misc"]["broker"]:
        # execute user-defined alert filters
        with timer(f"Filtering of {objectId} {candid}", alert_worker.verbose > 1):
            passed_filters = alert_filter__user_defined(
                alert_worker.mongo.db, alert_worker.filter_templates, alert
            )

        # post to SkyPortal
        alert_worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

    # clean up after thyself
    del record, alert, prv_candidates


class AlertConsumer:
    """
    Creates an alert stream Kafka consumer for a given topic.
    """

    def __init__(self, topic, dask_client, **kwargs):

        self.verbose = kwargs.get("verbose", 2)

        self.dask_client = dask_client

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

        # set up own mongo client
        self.collection_alerts = config["database"]["collections"]["alerts_ztf"]

        self.mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=self.verbose,
        )

        # create indexes
        if config["database"]["build_indexes"]:
            for index in config["database"]["indexes"][self.collection_alerts]:
                try:
                    ind = [tuple(ii) for ii in index["fields"]]
                    self.mongo.db[self.collection_alerts].create_index(
                        keys=ind,
                        name=index["name"],
                        background=True,
                        unique=index["unique"],
                    )
                except Exception as e:
                    log(e)

    @staticmethod
    def decode_message(msg):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """
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

    def poll(self):
        """
        Polls Kafka broker to consume a topic.

        :return:
        """
        msg = self.consumer.poll()

        if msg is None:
            log("Caught error: msg is None")

        if msg.error():
            log(f"Caught error: {msg.error()}")
            raise EopError(msg)

        elif msg is not None:
            try:
                # decode avro packet
                with timer("Decoding alert", self.verbose > 1):
                    msg_decoded = self.decode_message(msg)

                for record in msg_decoded:
                    # submit only unprocessed alerts:
                    if (
                        self.mongo.db[self.collection_alerts].count_documents(
                            {"candid": record["candid"]}, limit=1
                        )
                        == 0
                    ):
                        with timer(
                            f"Submitting alert {record['objectId']} {record['candid']} for processing",
                            self.verbose > 1,
                        ):
                            future = self.dask_client.submit(
                                process_alert, record, self.topic, pure=True
                            )
                            dask.distributed.fire_and_forget(future)
                            future.release()
                            del future

            except Exception as e:
                log(e)
                _err = traceback.format_exc()
                log(_err)


class AlertWorker:
    """Tools to handle alert processing: database ingestion, filtering, ml'ing, cross-matches, reporting to SP"""

    def __init__(self, **kwargs):

        self.verbose = kwargs.get("verbose", 2)
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
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=self.verbose,
        )

        # ML models
        self.ml_models = dict()
        for model in config["ml_models"]:
            try:
                model_version = config["ml_models"][model]["version"]
                # todo: allow other formats such as SavedModel
                model_filepath = os.path.join(
                    config["path"]["ml_models"], f"{model}.{model_version}.h5"
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
        if not config["misc"]["broker"]:
            return

        # session to talk to SkyPortal
        self.session = requests.Session()
        self.session_headers = {
            "Authorization": f"token {config['skyportal']['token']}"
        }

        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[405, 429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "POST", "PATCH"],
        )
        adapter = TimeoutHTTPAdapter(timeout=5, max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # get ZTF instrument id
        self.instrument_id = 1
        try:
            with timer("Getting ZTF instrument_id from SkyPortal", self.verbose > 1):
                response = self.api_skyportal("GET", "/api/instrument", {"name": "ZTF"})
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

        # load *active* user-defined alert filter templates and pre-populate them
        active_filters = self.get_active_filters()
        self.filter_templates = self.make_filter_templates(active_filters)

        # set up watchdog for periodic refresh of the filter templates, in case those change
        self.filter_monitor = threading.Thread(target=self.reload_filters)
        self.filter_monitor.start()

        log("Loaded user-defined filters:")
        log(self.filter_templates)

    def api_skyportal(self, method: str, endpoint: str, data: Optional[Mapping] = None):
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
            response = methods[method](
                f"{config['skyportal']['protocol']}://"
                f"{config['skyportal']['host']}:{config['skyportal']['port']}"
                f"{endpoint}",
                json=data,
                headers=self.session_headers,
            )

        return response

    @memoize
    def api_skyportal_get_group(self, group_id):
        return self.api_skyportal(
            "GET", f"/api/groups/{group_id}?includeGroupUsers=False"
        )

    def get_active_filters(self):
        """
        Fetch user-defined filters from own db marked as active

        :return:
        """
        # todo: query SP to make sure the filters still exist there and we're not out of sync;
        #       clean up if necessary
        return list(
            self.mongo.db[config["database"]["collections"]["filters"]].aggregate(
                [
                    {
                        "$match": {
                            "catalog": config["database"]["collections"]["alerts_ztf"],
                            "active": True,
                        }
                    },
                    {
                        "$project": {
                            "group_id": 1,
                            "filter_id": 1,
                            "permissions": 1,
                            "autosave": 1,
                            "update_annotations": 1,
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

    def make_filter_templates(self, active_filters: Sequence):
        """
        Make filter templates by adding metadata, prepending upstream aggregation stages and setting permissions

        :param active_filters:
        :return:
        """
        filter_templates = []
        for active_filter in active_filters:
            # collect additional info from SkyPortal
            with timer(
                f"Getting info on group id={active_filter['group_id']} from SkyPortal",
                self.verbose > 1,
            ):
                response = self.api_skyportal(
                    "GET", f"/api/groups/{active_filter['group_id']}"
                )
            if self.verbose > 1:
                log(response.json())
            if response.json()["status"] == "success":
                group_name = (
                    response.json()["data"]["nickname"]
                    if response.json()["data"]["nickname"] is not None
                    else response.json()["data"]["name"]
                )
                filter_name = [
                    filtr["name"]
                    for filtr in response.json()["data"]["filters"]
                    if filtr["id"] == active_filter["filter_id"]
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
            # set permissions
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
                "autosave": active_filter["autosave"],
                "update_annotations": active_filter["update_annotations"],
                "pipeline": deepcopy(pipeline),
            }

            filter_templates.append(filter_template)
        return filter_templates

    def reload_filters(self):
        """
        Helper function to periodically reload filters from SkyPortal

        :return:
        """
        while True:
            time.sleep(60 * 5)

            active_filters = self.get_active_filters()
            self.filter_templates = self.make_filter_templates(active_filters)

    @staticmethod
    def alert_mongify(alert: Mapping):
        """
        Prepare a raw ZTF alert for ingestion into MongoDB:
          - add a placeholder for ML-based classifications
          - add coordinates for 2D spherical indexing and compute Galactic coordinates
          - cut off the prv_candidates section

        :param alert:
        :return:
        """

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

    def alert_post_candidate(self, alert: Mapping, filter_ids: Sequence):
        """
        Post a ZTF alert as a candidate for filters on SkyPortal
        :param alert:
        :param filter_ids:
        :return:
        """
        # post metadata with all filter_ids in single call to /api/candidates
        alert_thin = {
            "id": alert["objectId"],
            "ra": alert["candidate"].get("ra"),
            "dec": alert["candidate"].get("dec"),
            "score": alert["candidate"].get("drb", alert["candidate"]["rb"]),
            "filter_ids": filter_ids,
            "passing_alert_id": alert["candid"],
            "passed_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "origin": "Kowalski",
        }
        if self.verbose > 1:
            log(alert_thin)

        with timer(
            f"Posting metadata of {alert['objectId']} {alert['candid']} to SkyPortal",
            self.verbose > 1,
        ):
            response = self.api_skyportal("POST", "/api/candidates", alert_thin)
        if response.json()["status"] == "success":
            log(f"Posted {alert['objectId']} {alert['candid']} metadata to SkyPortal")
        else:
            log(
                f"Failed to post {alert['objectId']} {alert['candid']} metadata to SkyPortal"
            )
            log(response.json())

    def alert_post_source(self, alert: Mapping, group_ids: Sequence):
        """
        Save a ZTF alert as a source to groups on SkyPortal

        :param alert:
        :param group_ids:
        :return:
        """
        # save source
        alert_thin = {
            "id": alert["objectId"],
            "group_ids": group_ids,
            "origin": "Kowalski",
        }
        if self.verbose > 1:
            log(alert_thin)

        with timer(
            f"Saving {alert['objectId']} {alert['candid']} as a Source on SkyPortal",
            self.verbose > 1,
        ):
            response = self.api_skyportal("POST", "/api/sources", alert_thin)
        if response.json()["status"] == "success":
            log(f"Saved {alert['objectId']} {alert['candid']} as a Source on SkyPortal")
        else:
            log(
                f"Failed to save {alert['objectId']} {alert['candid']} as a Source on SkyPortal"
            )
            log(response.json())

    def alert_post_annotations(self, alert: Mapping, passed_filters: Sequence):
        """
        Post annotations to SkyPortal for an alert that passed user-defined filters

        :param alert:
        :param passed_filters:
        :return:
        """
        for passed_filter in passed_filters:
            annotations = {
                "obj_id": alert["objectId"],
                "origin": f"{passed_filter.get('group_name')}:{passed_filter.get('filter_name')}",
                "data": passed_filter.get("data", dict()).get("annotations", dict()),
                "group_ids": [passed_filter.get("group_id")],
            }
            if len(annotations["data"]) > 0:
                with timer(
                    f"Posting annotation for {alert['objectId']} {alert['candid']} to SkyPortal",
                    self.verbose > 1,
                ):
                    response = self.api_skyportal(
                        "POST", "/api/annotation", annotations
                    )
                if response.json()["status"] == "success":
                    log(f"Posted {alert['objectId']} annotation to SkyPortal")
                else:
                    log(f"Failed to post {alert['objectId']} annotation to SkyPortal")
                    log(response.json())

    def alert_put_annotations(self, alert: Mapping, passed_filters: Sequence):
        """
        Update annotations on SkyPortal for an alert that passed user-defined filters

        :param alert:
        :param passed_filters:
        :return:
        """
        # first need to learn existing annotation id's and corresponding author id's to use with the PUT call
        with timer(
            f"Getting annotations for {alert['objectId']} from SkyPortal",
            self.verbose > 1,
        ):
            response = self.api_skyportal(
                "GET", f"/api/sources/{alert['objectId']}/annotations"
            )
        if response.json()["status"] == "success":
            log(f"Got {alert['objectId']} annotations from SkyPortal")
        else:
            log(f"Failed to get {alert['objectId']} annotations from SkyPortal")
            log(response.json())
            return False
        existing_annotations = {
            annotation["origin"]: {
                "annotation_id": annotation["id"],
                "author_id": annotation["author_id"],
            }
            for annotation in response.json()["data"]
        }

        for passed_filter in passed_filters:
            origin = (
                f"{passed_filter.get('group_name')}:{passed_filter.get('filter_name')}"
            )

            # no annotation exists on SkyPortal for this object? just post then
            if origin not in existing_annotations:
                self.alert_post_annotations(alert, [passed_filter])
                continue

            annotations = {
                "author_id": existing_annotations[origin]["author_id"],
                "obj_id": alert["objectId"],
                "origin": origin,
                "data": passed_filter.get("data", dict()).get("annotations", dict()),
                "group_ids": [passed_filter.get("group_id")],
            }
            if len(annotations["data"]) > 0 and passed_filter.get(
                "update_annotations", False
            ):
                with timer(
                    f"Putting annotation for {alert['objectId']} {alert['candid']} to SkyPortal",
                    self.verbose > 1,
                ):
                    response = self.api_skyportal(
                        "PUT",
                        f"/api/annotation/{existing_annotations[origin]['annotation_id']}",
                        annotations,
                    )
                if response.json()["status"] == "success":
                    log(f"Posted {alert['objectId']} annotation to SkyPortal")
                else:
                    log(f"Failed to post {alert['objectId']} annotation to SkyPortal")
                    log(response.json())

    def alert_post_thumbnails(self, alert: Mapping):
        """
        Post ZTF alert thumbnails to SkyPortal

        :param alert:
        :return:
        """
        for ttype, ztftype in [
            ("new", "Science"),
            ("ref", "Template"),
            ("sub", "Difference"),
        ]:
            with timer(
                f"Making {ztftype} thumbnail for {alert['objectId']} {alert['candid']}",
                self.verbose > 1,
            ):
                thumb = make_thumbnail(alert, ttype, ztftype)

            with timer(
                f"Posting {ztftype} thumbnail for {alert['objectId']} {alert['candid']} to SkyPortal",
                self.verbose > 1,
            ):
                response = self.api_skyportal("POST", "/api/thumbnail", thumb)

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
        with timer(
            f"Making alert photometry of {alert['objectId']} {alert['candid']}",
            self.verbose > 1,
        ):
            df_photometry = make_photometry(alert)

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
                    "flux": df_photometry.loc[pid_mask, "flux"].tolist(),
                    "fluxerr": df_photometry.loc[pid_mask, "fluxerr"].tolist(),
                    "zp": df_photometry.loc[pid_mask, "zp"].tolist(),
                    "magsys": df_photometry.loc[pid_mask, "zpsys"].tolist(),
                    "filter": df_photometry.loc[pid_mask, "ztf_filter"].tolist(),
                    "ra": df_photometry.loc[pid_mask, "ra"].tolist(),
                    "dec": df_photometry.loc[pid_mask, "dec"].tolist(),
                }

                if len(photometry.get("mag", ())) > 0:
                    with timer(
                        f"Posting photometry of {alert['objectId']} {alert['candid']}, "
                        f"program_id={pid} to SkyPortal",
                        self.verbose > 1,
                    ):
                        response = self.api_skyportal(
                            "PUT", "/api/photometry", photometry
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
        - if candidate does not exist and len(passed_filters) > 0
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
        with timer(
            f"Checking if {alert['objectId']} is Candidate in SkyPortal",
            self.verbose > 1,
        ):
            response = self.api_skyportal(
                "HEAD", f"/api/candidates/{alert['objectId']}"
            )
        is_candidate = response.status_code == 200
        if self.verbose > 1:
            log(
                f"{alert['objectId']} {'is' if is_candidate else 'is not'} Candidate in SkyPortal"
            )
        with timer(
            f"Checking if {alert['objectId']} is Source in SkyPortal", self.verbose > 1
        ):
            response = self.api_skyportal("HEAD", f"/api/sources/{alert['objectId']}")
        is_source = response.status_code == 200
        if self.verbose > 1:
            log(
                f"{alert['objectId']} {'is' if is_source else 'is not'} Source in SkyPortal"
            )

        # obj does not exit in SP and passed at least one filter:
        if (not is_candidate) and (not is_source) and (len(passed_filters) > 0):
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

            # post source if autosave=True
            autosave_group_ids = [
                f.get("group_id") for f in passed_filters if f.get("autosave", False)
            ]
            if len(autosave_group_ids) > 0:
                self.alert_post_source(alert, autosave_group_ids)

        # obj exists in SP:
        else:
            if len(passed_filters) > 0:
                filter_ids = [f.get("filter_id") for f in passed_filters]

                # post candidate with new filter ids
                self.alert_post_candidate(alert, filter_ids)

                # put annotations
                self.alert_put_annotations(alert, passed_filters)

            groups = deepcopy(passed_filters)
            group_ids = [f.get("group_id") for f in groups]
            # already saved as a source?
            if is_source:
                # get info on the corresponding groups:
                with timer(
                    f"Getting source groups info on {alert['objectId']} from SkyPortal",
                    self.verbose > 1,
                ):
                    response = self.api_skyportal(
                        "GET", f"/api/sources/{alert['objectId']}/groups"
                    )
                if response.json()["status"] == "success":
                    existing_groups = response.json()["data"]
                    existing_group_ids = [g["id"] for g in existing_groups]
                    # get group permissions for alert photometry access
                    for group_id in set(existing_group_ids) - set(group_ids):
                        with timer(
                            f"Getting info on group id={group_id} from SkyPortal",
                            self.verbose > 1,
                        ):
                            # memoize under the assumption that permissions change extremely rarely
                            response = self.api_skyportal_get_group(group_id)
                        if self.verbose > 1:
                            log(response.json())
                        if response.json()["status"] == "success":
                            # only public data (programid=1) by default:
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

                    # post source if autosave=True and not already saved
                    autosave_group_ids = [
                        f.get("group_id")
                        for f in passed_filters
                        if f.get("autosave", False)
                        and (f.get("group_id") not in existing_group_ids)
                    ]
                    if len(autosave_group_ids) > 0:
                        self.alert_post_source(alert, autosave_group_ids)
                else:
                    log(f"Failed to get source groups info on {alert['objectId']}")
            else:
                # post source if autosave=True
                autosave_group_ids = [
                    f.get("group_id")
                    for f in passed_filters
                    if f.get("autosave", False)
                ]
                if len(autosave_group_ids) > 0:
                    self.alert_post_source(alert, autosave_group_ids)

            # post alert photometry with all group_ids in single call to /api/photometry
            alert["prv_candidates"] = prv_candidates

            self.alert_put_photometry(alert, groups)


class WorkerInitializer(dask.distributed.WorkerPlugin):
    def __init__(self, *args, **kwargs):
        self.alert_worker = None

    def setup(self, worker: dask.distributed.Worker):
        self.alert_worker = AlertWorker()


def topic_listener(
    topic,
    bootstrap_servers: str,
    offset_reset: str = "earliest",
    group: str = None,
    test: bool = False,
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

    # Configure dask client
    dask_client = dask.distributed.Client(
        address=f"{config['dask']['host']}:{config['dask']['scheduler_port']}"
    )

    # init each worker with AlertWorker instance
    worker_initializer = WorkerInitializer()
    dask_client.register_worker_plugin(worker_initializer, name="worker-init")

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
    stream_reader = AlertConsumer(topic, dask_client, **conf)

    while True:
        try:
            # poll!
            stream_reader.poll()

        except EopError as e:
            # Write when reaching end of partition
            log(e.message)
            if test:
                # when testing, terminate once reached end of partition:
                sys.exit()
        except IndexError:
            log("Data cannot be decoded\n")
        except UnicodeDecodeError:
            log("Unexpected data format received\n")
        except KeyboardInterrupt:
            log("Aborted by user\n")
            sys.exit()
        except Exception as e:
            log(str(e))
            _err = traceback.format_exc()
            log(_err)
            sys.exit()


def watchdog(obs_date: str = None, test: bool = False):
    """
        Watchdog for topic listeners

    :param obs_date: observing date: YYYYMMDD
    :param test: test mode
    :return:
    """

    init_db_sync(config=config, verbose=True)

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
            log(f"Topics: {topics_tonight}")

            for t in topics_tonight:
                if t not in topics_on_watch:
                    log(f"Starting listener thread for {t}")
                    offset_reset = config["kafka"]["default.topic.config"][
                        "auto.offset.reset"
                    ]
                    if not test:
                        bootstrap_servers = config["kafka"]["bootstrap.servers"]
                    else:
                        bootstrap_servers = config["kafka"]["bootstrap.test.servers"]
                    group = config["kafka"]["group"]

                    topics_on_watch[t] = multiprocessing.Process(
                        target=topic_listener,
                        args=(t, bootstrap_servers, offset_reset, group, test),
                    )
                    topics_on_watch[t].daemon = True
                    topics_on_watch[t].start()

                else:
                    log(f"Performing thread health check for {t}")
                    try:
                        if not topics_on_watch[t].is_alive():
                            log(f"Thread {t} died, removing")
                            # topics_on_watch[t].terminate()
                            topics_on_watch.pop(t, None)
                        else:
                            log(f"Thread {t} appears normal")
                    except Exception as _e:
                        log(f"Failed to perform health check: {_e}")
                        pass

            if test:
                time.sleep(120)
                # when testing, wait for topic listeners to pull all the data, then break
                # fixme: do this more gracefully
                for t in topics_on_watch:
                    topics_on_watch[t].kill()
                break

        except Exception as e:
            log(str(e))
            _err = traceback.format_exc()
            log(str(_err))

        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kowalski's ZTF Alert Broker")
    parser.add_argument("--obsdate", help="observing date YYYYMMDD")
    parser.add_argument("--test", help="listen to the test stream", action="store_true")

    args = parser.parse_args()

    watchdog(obs_date=args.obsdate, test=args.test)
