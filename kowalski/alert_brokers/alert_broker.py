__all__ = ["AlertConsumer", "AlertWorker", "EopError"]

import base64
import datetime
import gzip
import io
import os
import pathlib
import sys
import traceback
from ast import literal_eval
from copy import deepcopy
from typing import Mapping, Optional, Sequence

import confluent_kafka
import dask.distributed
import fastavro
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from astropy.io import fits
from astropy.visualization import (
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
)
from requests.packages.urllib3.util.retry import Retry

from kowalski.config import load_config
from kowalski.log import log
from kowalski.utils import (
    Mongo,
    TimeoutHTTPAdapter,
    ZTFAlert,
    deg2dms,
    deg2hms,
    great_circle_distance,
    in_ellipse,
    memoize,
    radec2lb,
    time_stamp,
    timer,
    retry,
)

# Tensorflow is problematic for Mac's currently, so we can add an option to disable it
USE_TENSORFLOW = os.environ.get("USE_TENSORFLOW", True) in [
    "True",
    "t",
    "true",
    "1",
    True,
    1,
]

if USE_TENSORFLOW:
    import tensorflow as tf
    from tensorflow.keras.models import load_model

    tf.config.optimizer.set_jit(True)


""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


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


class AlertConsumer:
    """
    Creates an alert stream Kafka consumer for a given topic.
    """

    def __init__(self, topic: str, dask_client: dask.distributed.Client, **kwargs):

        self.verbose = kwargs.get("verbose", 2)

        self.instrument = kwargs.pop("instrument", "ZTF")

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
        log(f"Successfully subscribed to {topic}")

        # set up own mongo client
        self.collection_alerts = config["database"]["collections"][
            f"alerts_{self.instrument.lower()}"
        ]

        self.mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            srv=config["database"]["srv"],
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

        log("Finished AlertConsumer setup")

    @staticmethod
    def read_schema_data(bytes_io):
        """Read data that already has an Avro schema.

        :param bytes_io: `_io.BytesIO` Data to be decoded.
        :return: `dict` Decoded data.
        """
        bytes_io.seek(0)
        message = fastavro.reader(bytes_io)
        return message

    @classmethod
    def decode_message(cls, msg):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """
        message = msg.value()
        decoded_msg = message

        try:
            bytes_io = io.BytesIO(message)
            decoded_msg = cls.read_schema_data(bytes_io)
        except AssertionError:
            decoded_msg = None
        except IndexError:
            literal_msg = literal_eval(
                str(message, encoding="utf-8")
            )  # works to give bytes
            bytes_io = io.BytesIO(literal_msg)  # works to give <class '_io.BytesIO'>
            decoded_msg = cls.read_schema_data(bytes_io)  # yields reader
        except Exception:
            decoded_msg = message
        finally:
            return decoded_msg

    @staticmethod
    def process_alert(alert: Mapping, topic: str):
        """Alert brokering task run by dask.distributed workers

        :param alert: decoded alert from Kafka stream
        :param topic: Kafka stream topic name for bookkeeping
        :return:
        """
        raise NotImplementedError("Must be implemented in subclass")

    def submit_alert(self, record: Mapping, topic: str):
        with timer(
            f"Submitting alert {record['objectId']} {record['candid']} for processing",
            self.verbose > 1,
        ):
            future = self.dask_client.submit(
                self.process_alert, record, self.topic, pure=True
            )
            dask.distributed.fire_and_forget(future)
            future.release()
            del future
        return

    def poll(self):
        """Polls Kafka broker to consume a topic."""
        msg = self.consumer.poll()

        if msg is None:
            log("Caught error: msg is None")

        if msg.error():
            # reached end of topic
            log(f"Caught error: {msg.error()}")
            raise EopError(msg)

        elif msg is not None:
            try:
                # decode avro packet
                with timer("Decoding alert", self.verbose > 1):
                    msg_decoded = self.decode_message(msg)

                for record in msg_decoded:
                    if (
                        retry(self.mongo.db[self.collection_alerts].count_documents)(
                            {"candid": record["candid"]}, limit=1
                        )
                        == 0
                    ):

                        self.submit_alert(record, self.topic)

                # clean up after thyself
                del msg_decoded

            except Exception as e:
                print("Error in poll!")
                log(e)
                _err = traceback.format_exc()
                log(_err)

        # clean up after thyself
        del msg


class AlertWorker:
    """Tools to handle alert processing:
    database ingestion, filtering, ml'ing, cross-matches, reporting to SP"""

    def __init__(self, **kwargs):

        self.verbose = kwargs.get("verbose", 2)
        self.config = config

        self.instrument = kwargs.get("instrument", "ZTF")

        # Kowalski version
        path_version_file = pathlib.Path(__file__).parent.absolute() / "version.txt"
        version = f"v{self.config['server']['version']}"
        if path_version_file.exists():
            with open(
                pathlib.Path(__file__).parent.absolute() / "version.txt", "r"
            ) as version_file:
                version = version_file.read().strip()

        # MongoDB collections to store the alerts:
        self.collection_alerts = self.config["database"]["collections"][
            f"alerts_{self.instrument.lower()}"
        ]
        self.collection_alerts_aux = self.config["database"]["collections"][
            f"alerts_{self.instrument.lower()}_aux"
        ]

        self.mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            srv=config["database"]["srv"],
            verbose=self.verbose,
        )

        # ML models
        self.ml_models = dict()
        self.allowed_features = (
            config["ml"].get(self.instrument, {}).get("allowed_features", "()")
        )
        if isinstance(self.allowed_features, str):
            try:
                self.allowed_features = literal_eval(self.allowed_features)
            except Exception:
                log(
                    f"Invalid format for ml.{self.instrument}.allowed_features, must be a tuple of strings"
                )
                self.allowed_features = ()
        if len(self.allowed_features) == 0:
            log(
                f"No ML models will be used: ml.{self.instrument}.allowed_features is empty/missing"
            )

        if USE_TENSORFLOW and len(self.allowed_features) > 0:
            for model in config["ml"].get(self.instrument, {}).get("models", []):
                try:
                    if not set(
                        config["ml"][self.instrument]["models"][model].keys()
                    ).issubset(
                        {
                            "version",
                            "feature_names",
                            "feature_norms",
                            "triplet",
                            "format",
                            "order",
                            "url",
                        }
                    ):
                        raise ValueError(
                            f"Invalid keys in ml.{self.instrument}.models.{model}, must be 'version', 'feature_names', 'feature_norms', 'triplet','format', and 'order', and 'url' (optional)"
                        )

                    model_version = config["ml"][self.instrument]["models"][model][
                        "version"
                    ]
                    model_feature_names = config["ml"][self.instrument]["models"][
                        model
                    ].get("feature_names", False)
                    model_feature_norms = config["ml"][self.instrument]["models"][
                        model
                    ].get("feature_norms", None)
                    model_triplet = config["ml"][self.instrument]["models"][model].get(
                        "triplet", False
                    )
                    model_format = config["ml"][self.instrument]["models"][model].get(
                        "format", "h5"
                    )
                    model_order = config["ml"][self.instrument]["models"][model].get(
                        "order", ["features", "triplet"]
                    )
                    if model_format not in ["h5", "pb"]:
                        raise ValueError(
                            f"Invalid ml.{self.instrument}.models.{model}.format: {model_format}, must be 'h5' or 'pb'"
                        )
                    if model_format == "pb":
                        model_format = "/"
                    else:
                        model_format = f".{model_format}"
                    if model_feature_names is True:
                        model_feature_names = self.allowed_features
                    if isinstance(model_feature_names, str):
                        try:
                            model_feature_names = literal_eval(model_feature_names)
                        except Exception:
                            raise ValueError(
                                f"Invalid ml.{self.instrument}.models.{model}.feature_names, must be a tuple of strings"
                            )

                    if model_feature_names is False and model_triplet is False:
                        raise ValueError(
                            f"ml.{self.instrument}.models.{model}: both 'feature_names' or 'triplet' cannot be False for model {model}"
                        )
                    if not isinstance(model_feature_names, bool) and not isinstance(
                        model_feature_names, tuple
                    ):
                        raise ValueError(
                            f"ml.{self.instrument}.models.{model}.feature_names must be either a bool or a tuple, got {type(model_feature_names)}"
                        )

                    if not set(
                        model_feature_names
                        if isinstance(model_feature_names, tuple)
                        else []
                    ).issubset(set(self.allowed_features)):
                        raise ValueError(
                            f"ml.{self.instrument}.models.{model}.feature_names must be a subset of the {self.allowed_features}"
                        )
                    if model_feature_norms is not None and not isinstance(
                        model_feature_norms, dict
                    ):
                        raise ValueError(
                            f"ml.{self.instrument}.models.{model}.feature_norms must be a dict, or None, got {type(model_feature_norms)}"
                        )
                    if model_feature_norms is not None and not set(
                        model_feature_norms.keys()
                    ).issubset(set(model_feature_names)):
                        raise ValueError(
                            f"ml.{self.instrument}.models.{model}.feature_norms keys must be a subset of model_feature_names"
                        )
                    if not isinstance(model_triplet, bool):
                        raise ValueError(
                            f"ml.{self.instrument}.models.{model}.triplet must be a bool, got {type(model_triplet)}"
                        )
                    if not isinstance(model_version, str) or model_version == "":
                        raise ValueError(
                            f"ml.{self.instrument}.models.{model}.version must be a non empty string, got {type(model_version)}"
                        )

                    # todo: allow other formats such as SavedModel
                    model_filepath = os.path.join(
                        f"models/{self.instrument.lower()}",
                        f"{model}.{model_version}{model_format}",
                    )
                    self.ml_models[model] = {
                        "model": load_model(model_filepath),
                        "version": model_version,
                        "feature_names": model_feature_names,
                        "feature_norms": model_feature_norms,
                        "triplet": model_triplet,
                        "order": model_order,
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
            "Authorization": f"token {config['skyportal']['token']}",
            "User-Agent": f"Kowalski {version}",
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

        # get instrument id
        self.instrument_id = 1
        with timer(
            f"Getting {self.instrument} instrument_id from SkyPortal", self.verbose > 1
        ):
            response = self.api_skyportal(
                "GET", "/api/instrument", {"name": self.instrument}
            )
        if response.json()["status"] == "success" and len(response.json()["data"]) > 0:
            self.instrument_id = response.json()["data"][0]["id"]
            log(
                f"Got {self.instrument} instrument_id from SkyPortal: {self.instrument_id}"
            )
        else:
            log(f"Failed to get {self.instrument} instrument_id from SkyPortal")
            raise ValueError(
                f"Failed to get {self.instrument} instrument_id from SkyPortal"
            )
        log("AlertWorker setup complete")

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

    @staticmethod
    def alert_mongify(alert: Mapping):
        """
        Prepare a raw alert for ingestion into MongoDB:
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

    def make_thumbnail(
        self, alert: Mapping, skyportal_type: str, alert_packet_type: str
    ):
        """
        Convert lossless FITS cutouts from ZTF-like alerts into PNGs

        :param alert: ZTF-like alert packet/dict
        :param skyportal_type: <new|ref|sub> thumbnail type expected by SkyPortal
        :param alert_packet_type: <Science|Template|Difference> survey naming
        :return:
        """
        alert = deepcopy(alert)

        cutout_data = alert[f"cutout{alert_packet_type}"]
        if self.instrument == "ZTF":
            cutout_data = cutout_data["stampData"]
        with gzip.open(io.BytesIO(cutout_data), "rb") as f:
            with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                image_data = hdu[0].data

        # Survey-specific transformations to get North up and West on the right
        if self.instrument == "ZTF":
            image_data = np.flipud(image_data)
        elif self.instrument == "PGIR":
            image_data = np.rot90(np.fliplr(image_data), 3)

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

        norm = ImageNormalize(
            img,
            stretch=LinearStretch()
            if alert_packet_type == "Difference"
            else LogStretch(),
        )
        img_norm = norm(img)
        normalizer = AsymmetricPercentileInterval(
            lower_percentile=1, upper_percentile=100
        )
        vmin, vmax = normalizer.get_limits(img_norm)
        ax.imshow(img_norm, cmap="bone", origin="lower", vmin=vmin, vmax=vmax)
        plt.savefig(buff, dpi=42)

        buff.seek(0)
        plt.close("all")

        thumbnail_dict = {
            "obj_id": alert["objectId"],
            "data": base64.b64encode(buff.read()).decode("utf-8"),
            "ttype": skyportal_type,
        }

        return thumbnail_dict

    @staticmethod
    def make_triplet(alert: Mapping, to_tpu: bool = False):
        """
        Make an L2-normalized cutout triplet out of an alert

        :param alert:
        :param to_tpu:
        :return:
        """
        cutout_dict = dict()

        for cutout in ("science", "template", "difference"):
            cutout_data = alert[f"cutout{cutout.capitalize()}"]["stampData"]

            # unzip
            with gzip.open(io.BytesIO(cutout_data), "rb") as f:
                with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                    data = hdu[0].data
                    # replace nans with zeros
                    cutout_dict[cutout] = np.nan_to_num(data)
                    # L2-normalize
                    cutout_dict[cutout] /= np.linalg.norm(cutout_dict[cutout])

            # pad to 63x63 if smaller
            shape = cutout_dict[cutout].shape
            if shape != (63, 63):
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

    def make_photometry(self, alert: Mapping, jd_start: float = None):
        """
        Make a de-duplicated pandas.DataFrame with photometry of alert['objectId']

        :param alert: ZTF-like alert packet/dict
        :param jd_start:
        :return:
        """
        alert = deepcopy(alert)
        df_candidate = pd.DataFrame(alert["candidate"], index=[0])

        df_prv_candidates = pd.DataFrame(alert["prv_candidates"])
        df_light_curve = pd.concat(
            [df_candidate, df_prv_candidates], ignore_index=True, sort=False
        )

        if self.instrument == "ZTF":
            ztf_filters = {1: "ztfg", 2: "ztfr", 3: "ztfi"}
            df_light_curve["filter"] = df_light_curve["fid"].apply(
                lambda x: ztf_filters[x]
            )
        elif self.instrument == "PGIR":
            # fixme: PGIR uses 2massj, which is not in sncosmo as of 20210803
            #        cspjs seems to be close/good enough as an approximation
            df_light_curve["filter"] = "cspjs"
        elif self.instrument == "WNTR":
            # 20220818: added WNTR
            # 20220929: nir bandpasses have been added to sncosmo
            nir_filters = {0: "ps1::y", 1: "2massj", 2: "2massh", 3: "2massks"}
            df_light_curve["filter"] = df_light_curve["fid"].apply(
                lambda x: nir_filters[x]
            )

        df_light_curve["magsys"] = "ab"
        df_light_curve["mjd"] = df_light_curve["jd"] - 2400000.5

        df_light_curve["mjd"] = df_light_curve["mjd"].apply(lambda x: np.float64(x))
        df_light_curve["magpsf"] = df_light_curve["magpsf"].apply(
            lambda x: np.float32(x)
        )
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
            lambda x: 1.0 if x in [True, 1, "y", "Y", "t", "1"] else -1.0
        )

        # step 2: calculate the flux normalized to an arbitrary AB zeropoint of
        # 23.9 (results in flux in uJy)
        df_light_curve["flux"] = coeff * 10 ** (
            -0.4 * (df_light_curve["magpsf"] - 23.9)
        )

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

        # convert all nan values to None
        df_light_curve = df_light_curve.replace({np.nan: None})

        return df_light_curve

    def alert_filter__ml(self, alert: Mapping, alert_history: list = []) -> dict:
        """Execute ML models on a ZTF-like alert

        :param alert:
        :return:
        """

        if self.ml_models is None or len(self.ml_models) == 0:
            return dict()

        scores = dict()

        if self.instrument == "ZTF":
            try:
                with timer("ZTFAlert(alert)"):
                    ztf_alert = ZTFAlert(alert, alert_history, self.ml_models)

                for model_name in self.ml_models.keys():
                    inputs = {}
                    features, triplet, score = None, None, None
                    try:
                        with timer(f"Prepping features for {model_name}"):
                            if self.ml_models[model_name]["feature_names"] is not False:
                                features = ztf_alert.data["features"][model_name]
                                inputs["features"] = np.expand_dims(
                                    features, axis=[0, -1]
                                )
                            if self.ml_models[model_name]["triplet"] is not False:
                                triplet = ztf_alert.data["triplet"]
                                inputs["triplet"] = np.expand_dims(triplet, axis=[0])
                            if len(inputs.keys()) == 1:
                                inputs = inputs[list(inputs.keys())[0]]
                            else:
                                inputs = [
                                    inputs[k]
                                    for k in self.ml_models[model_name]["order"]
                                ]

                        with timer(model_name):
                            score = self.ml_models[model_name]["model"].predict(
                                x=inputs
                            )[0]
                            scores[model_name] = float(score)
                            scores[f"{model_name}_version"] = self.ml_models[
                                model_name
                            ]["version"]
                    except Exception as e:
                        log(
                            f"Failed to run ML model {model_name} on alert {alert['objectId']}: {e}"
                        )

                    # clean up after thyself
                    del inputs, features, triplet, score

            except Exception as e:
                log(f"Failed to run ML models on alert {alert['objectId']}: {e}")

        elif self.instrument == "PGIR":
            # TODO
            pass

        # clean up after thyself
        del alert_history

        return scores

    def alert_filter__xmatch(self, alert: Mapping) -> dict:
        """Cross-match alerts against external catalogs"""

        xmatches = dict()

        try:
            ra = float(alert["candidate"]["ra"])
            dec = float(alert["candidate"]["dec"])
            ra_geojson = float(alert["candidate"]["ra"])
            # geojson-friendly ra:
            ra_geojson -= 180.0
            dec_geojson = float(alert["candidate"]["dec"])

            """ catalogs """
            matches = []
            cross_match_config = config["database"]["xmatch"][self.instrument]
            for catalog in cross_match_config:
                try:
                    # if the catalog has "distance", "ra", "dec" in its config, then it is a catalog with distance
                    if cross_match_config[catalog].get("use_distance", False):
                        matches = self.alert_filter__xmatch_distance(
                            ra,
                            dec,
                            ra_geojson,
                            dec_geojson,
                            catalog,
                            cross_match_config,
                        )
                    else:
                        matches = self.alert_filter__xmatch_no_distance(
                            ra_geojson, dec_geojson, catalog, cross_match_config
                        )
                except Exception as e:
                    log(f"Failed to cross-match {catalog}: {str(e)}")
                    matches = []
                xmatches[catalog] = matches

            # clean up after thyself
            del ra, dec, ra_geojson, dec_geojson, matches, cross_match_config

        except Exception as e:
            log(f"Failed catalogs cross-match: {str(e)}")

        return xmatches

    def alert_filter__xmatch_no_distance(
        self,
        ra_geojson: float,
        dec_geojson: float,
        catalog: str,
        cross_match_config: dict,
    ) -> dict:
        """Cross-match alerts against external catalogs"""

        matches = []

        try:
            # cone search radius:
            catalog_cone_search_radius = float(
                cross_match_config[catalog]["cone_search_radius"]
            )
            # convert to rad:
            if cross_match_config[catalog]["cone_search_unit"] == "arcsec":
                catalog_cone_search_radius *= np.pi / 180.0 / 3600.0
            elif cross_match_config[catalog]["cone_search_unit"] == "arcmin":
                catalog_cone_search_radius *= np.pi / 180.0 / 60.0
            elif cross_match_config[catalog]["cone_search_unit"] == "deg":
                catalog_cone_search_radius *= np.pi / 180.0
            elif cross_match_config[catalog]["cone_search_unit"] == "rad":
                pass
            else:
                raise Exception(
                    f"Unknown cone search radius units for {catalog}."
                    " Must be one of [deg, rad, arcsec, arcmin]"
                )

            catalog_filter = cross_match_config[catalog]["filter"]
            catalog_projection = cross_match_config[catalog]["projection"]

            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra_geojson, dec_geojson],
                        catalog_cone_search_radius,
                    ]
                }
            }
            s = retry(self.mongo.db[catalog].find)(
                {**object_position_query, **catalog_filter}, {**catalog_projection}
            )
            matches = list(s)

        except Exception as e:
            log(str(e))

        return matches

    def alert_filter__xmatch_distance(
        self,
        ra: float,
        dec: float,
        ra_geojson: float,
        dec_geojson: float,
        catalog: str,
        cross_match_config: dict,
    ) -> dict:
        """
        Run cross-match with catalogs that have a distance value

        :param alert:
        :param catalog: name of the catalog (collection) to cross-match with
        :return:
        """

        matches = []

        try:
            catalog_cm_at_distance = cross_match_config[catalog]["cm_at_distance"]
            catalog_cm_low_distance = cross_match_config[catalog]["cm_low_distance"]
            # cone search radius:
            catalog_cone_search_radius = float(
                cross_match_config[catalog]["cone_search_radius"]
            )
            # convert to rad:
            if cross_match_config[catalog]["cone_search_unit"] == "arcsec":
                catalog_cone_search_radius *= np.pi / 180.0 / 3600.0
            elif cross_match_config[catalog]["cone_search_unit"] == "arcmin":
                catalog_cone_search_radius *= np.pi / 180.0 / 60.0
            elif cross_match_config[catalog]["cone_search_unit"] == "deg":
                catalog_cone_search_radius *= np.pi / 180.0
            elif cross_match_config[catalog]["cone_search_unit"] == "rad":
                pass

            catalog_filter = cross_match_config[catalog]["filter"]
            catalog_projection = cross_match_config[catalog]["projection"]

            # first do a coarse search of everything that is around
            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra_geojson, dec_geojson],
                        catalog_cone_search_radius,
                    ]
                }
            }
            galaxies = list(
                retry(self.mongo.db[catalog].find)(
                    {**object_position_query, **catalog_filter}, {**catalog_projection}
                )
            )

            distance_value = cross_match_config[catalog]["distance_value"]
            distance_method = cross_match_config[catalog]["distance_method"]

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
                "DistMpc": 0.778,
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
                "DistMpc": 0.869,
                "sfr_fuv": None,
                "mstar": 4502777.420493,
                "sfr_ha": 0,
                "coordinates": {"radec_str": ["01:33:50.8900", "30:39:36.800"]},
            }

            if distance_value == "z" or distance_method in ["redshift", "z"]:
                M31[distance_value] = M31["z"]
                M33[distance_value] = M33["z"]
            else:
                M31[distance_value] = M31["DistMpc"]
                M33[distance_value] = M33["DistMpc"]

            for galaxy in galaxies + [M31, M33]:
                try:
                    alpha1, delta01 = galaxy["ra"], galaxy["dec"]

                    redshift, distmpc = None, None
                    if distance_value == "z" or distance_method in [
                        "redshift",
                        "z",
                    ]:
                        redshift = galaxy[distance_value]

                        if redshift < 0.01:
                            # for nearby galaxies and galaxies with negative redshifts, do a `catalog_cm_low_distance` arc-minute cross-match
                            # (cross-match radius would otherwise get un-physically large for nearby galaxies)
                            cm_radius = catalog_cm_low_distance / 3600
                        else:
                            # For distant galaxies, set the cross-match radius to 30 kpc at the redshift of the host galaxy
                            cm_radius = (
                                catalog_cm_at_distance * (0.05 / redshift) / 3600
                            )
                    else:
                        distmpc = galaxy[distance_value]

                        if distmpc < 40:
                            # for nearby galaxies, do a `catalog_cm_low_distance` arc-minute cross-match
                            cm_radius = catalog_cm_low_distance / 3600
                        else:
                            # For distant galaxies, set the cross-match radius to 30 kpc at the distance (in Mpc) of the host galaxy
                            cm_radius = np.rad2deg(
                                np.arctan(catalog_cm_at_distance / (distmpc * 10**3))
                            )

                    in_galaxy = in_ellipse(ra, dec, alpha1, delta01, cm_radius, 1, 0)

                    if in_galaxy:
                        match = galaxy
                        distance_arcsec = round(
                            great_circle_distance(ra, dec, alpha1, delta01) * 3600,
                            2,
                        )
                        # also add a physical distance parameter for redshifts in the Hubble flow
                        if redshift is not None and redshift > 0.005:
                            distance_kpc = round(
                                great_circle_distance(ra, dec, alpha1, delta01)
                                * 3600
                                * (redshift / 0.05),
                                2,
                            )
                        elif distmpc is not None and distmpc > 0.005:
                            distance_kpc = round(
                                np.deg2rad(
                                    great_circle_distance(ra, dec, alpha1, delta01)
                                )
                                * distmpc
                                * 10**3,
                                2,
                            )
                        else:
                            distance_kpc = -1

                        match["coordinates"]["distance_arcsec"] = distance_arcsec
                        match["coordinates"]["distance_kpc"] = distance_kpc
                        matches.append(match)
                except Exception as e:
                    log(f"Could not crossmatch with galaxy {str(galaxy)} : {str(e)}")

            return matches

        except Exception as e:
            log(f"Could not crossmatch with ANY galaxies: {str(e)}")

        return matches

    def alert_filter__user_defined(
        self,
        filter_templates: Sequence,
        alert: Mapping,
        max_time_ms: int = 1000,
    ) -> list:
        """Evaluate user-defined filters

        :param filter_templates:
        :param alert:
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
                    retry(self.mongo.db[self.collection_alerts].aggregate)(
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

    def alert_post_candidate(self, alert: Mapping, filter_ids: Sequence):
        """Post an alert as a candidate for filters on SkyPortal

        :param alert:
        :param filter_ids:
        :return:
        """
        # post metadata with all filter_ids in single call to /api/candidates
        alert_thin = {
            "id": alert["objectId"],
            "ra": alert["candidate"].get("ra"),
            "dec": alert["candidate"].get("dec"),
            "score": alert["candidate"].get("drb", alert["candidate"].get("rb")),
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
        """Save an alert as a source to groups on SkyPortal

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
            try:
                response = self.api_skyportal("POST", "/api/sources", alert_thin)
                if response.json()["status"] == "success":
                    log(
                        f"Saved {alert['objectId']} {alert['candid']} as a Source on SkyPortal"
                    )
                else:
                    raise ValueError(response.json()["message"])
            except Exception as e:
                log(
                    f"Failed to save {alert['objectId']} {alert['candid']} as a Source on SkyPortal: {e}"
                )

    def alert_post_annotations(self, alert: Mapping, passed_filters: Sequence):
        """Post annotations to SkyPortal for an alert that passed user-defined filters

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
                    f"Posting annotation {annotations['origin']} for {alert['objectId']} {alert['candid']} to SkyPortal",
                    self.verbose > 1,
                ):
                    try:
                        response = self.api_skyportal(
                            "POST",
                            f"/api/sources/{alert['objectId']}/annotations",
                            annotations,
                        )
                        if response.json()["status"] == "success":
                            log(
                                f"Posted {alert['objectId']} {alert['candid']} annotation {annotations['origin']} to SkyPortal"
                            )
                        else:
                            raise ValueError(response.json()["message"])
                    except Exception as e:
                        log(
                            f"Failed to post {alert['objectId']} {alert['candid']} annotation {annotations['origin']} to SkyPortal: {e}"
                        )
                        continue

    def alert_put_annotations(self, alert: Mapping, passed_filters: Sequence):
        """Update annotations on SkyPortal for an alert that passed user-defined filters

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
                    try:
                        response = self.api_skyportal(
                            "PUT",
                            f"/api/sources/{alert['objectId']}"
                            f"/annotations/{existing_annotations[origin]['annotation_id']}",
                            annotations,
                        )
                        if response.json()["status"] == "success":
                            log(
                                f"Updated {alert['objectId']} annotation {origin} to SkyPortal"
                            )
                        else:
                            raise ValueError(response.json()["message"])
                    except Exception as e:
                        log(
                            f"Failed to put {alert['objectId']} {alert['candid']} annotation {origin} to SkyPortal: {e}"
                        )

    def alert_post_thumbnails(self, alert: Mapping):
        """Post alert thumbnails to SkyPortal

        :param alert:
        :return:
        """
        for ttype, istrument_type in [
            ("new", "Science"),
            ("ref", "Template"),
            ("sub", "Difference"),
        ]:
            with timer(
                f"Making {istrument_type} thumbnail for {alert['objectId']} {alert['candid']}",
                self.verbose > 1,
            ):
                thumb = self.make_thumbnail(alert, ttype, istrument_type)

            with timer(
                f"Posting {istrument_type} thumbnail for {alert['objectId']} {alert['candid']} to SkyPortal",
                self.verbose > 1,
            ):
                try:
                    response = self.api_skyportal("POST", "/api/thumbnail", thumb)
                    if response.json()["status"] == "success":
                        log(
                            f"Posted {alert['objectId']} {alert['candid']} {istrument_type} cutout to SkyPortal"
                        )
                    else:
                        raise ValueError(response.json()["message"])
                except Exception as e:
                    log(
                        f"Failed to post {alert['objectId']} {alert['candid']} {istrument_type} cutout to SkyPortal: {e}"
                    )
                    continue

    def alert_put_photometry(self, alert):
        """PUT photometry to SkyPortal

        :param alert:
        :return:
        """
        raise NotImplementedError(
            "Must be implemented in subclass, too survey-specific."
        )

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
          - post alert light curve in single PUT call to /api/photometry specifying stream_ids
        - if source exists:
          - get groups and check stream access
          - decide which points to post to what groups based on permissions
          - post alert light curve in single PUT call to /api/photometry specifying stream_ids

        :param alert: alert with a stripped-off prv_candidates section
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

        # obj does not exit in SP:
        if (not is_candidate) and (not is_source):
            # passed at least one filter?
            if len(passed_filters) > 0:
                # post candidate
                filter_ids = [f.get("filter_id") for f in passed_filters]
                self.alert_post_candidate(alert, filter_ids)

                # post annotations
                self.alert_post_annotations(alert, passed_filters)

                # post full light curve
                try:
                    alert["prv_candidates"] = list(
                        retry(self.mongo.db[self.collection_alerts_aux].find)(
                            {"_id": alert["objectId"]}, {"prv_candidates": 1}, limit=1
                        )
                    )[0]["prv_candidates"]
                except Exception as e:
                    # this should never happen, but just in case
                    log(e)
                    alert["prv_candidates"] = prv_candidates

                self.alert_put_photometry(alert)

                # post thumbnails
                self.alert_post_thumbnails(alert)

                # post source if autosave=True
                autosave_group_ids = [
                    f.get("group_id")
                    for f in passed_filters
                    if f.get("autosave", False)
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
                # post source if autosave=True and not is_source
                autosave_group_ids = [
                    f.get("group_id")
                    for f in passed_filters
                    if f.get("autosave", False)
                ]
                if len(autosave_group_ids) > 0:
                    self.alert_post_source(alert, autosave_group_ids)

            # post alert photometry in single call to /api/photometry
            alert["prv_candidates"] = prv_candidates

            self.alert_put_photometry(alert)
