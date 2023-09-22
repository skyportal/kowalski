import argparse
import datetime
import multiprocessing
import os
import subprocess
import sys
import threading
import time
import traceback
from abc import ABC
from copy import deepcopy
from typing import Mapping, Sequence

import dask.distributed
from kowalski.alert_brokers.alert_broker import AlertConsumer, AlertWorker, EopError
from bson.json_util import loads as bson_loads
from kowalski.utils import init_db_sync, timer
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


class TURBOAlertConsumer(AlertConsumer, ABC):
    """
    Creates an alert stream Kafka consumer for a given topic.
    """

    def __init__(self, topic: str, dask_client: dask.distributed.Client, **kwargs):
        super().__init__(topic, dask_client, **kwargs)

    @staticmethod
    def process_alert(alert: Mapping, topic: str):
        """Alert brokering task run by dask.distributed workers

        :param alert: decoded alert from Kafka stream
        :param topic: Kafka stream topic name for bookkeeping
        :return:
        """
        candid = alert["candid"]
        object_id = alert["objectId"]

        # get worker running current task
        worker = dask.distributed.get_worker()
        alert_worker = worker.plugins["worker-init"].alert_worker

        log(f"{topic} {object_id} {candid} {worker.address}")

        # return if this alert packet has already been processed and ingested into collection_alerts:
        if (
            alert_worker.mongo.db[alert_worker.collection_alerts].count_documents(
                {"candid": candid}, limit=1
            )
            == 1
        ):
            return

        # candid not in db, ingest decoded avro packet into db
        with timer(f"Mongification of {object_id} {candid}", alert_worker.verbose > 1):
            alert, prv_candidates = alert_worker.alert_mongify(alert)

        with timer(f"Ingesting {object_id} {candid}", alert_worker.verbose > 1):
            alert_worker.mongo.insert_one(
                collection=alert_worker.collection_alerts, document=alert
            )

        # prv_candidates: pop nulls - save space
        prv_candidates = [
            {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
            for prv_candidate in prv_candidates
        ]

        alert_aux, xmatches, xmatches_ztf, passed_filters = None, None, None, None
        # cross-match with external catalogs if objectId not in collection_alerts_aux:
        if (
            alert_worker.mongo.db[alert_worker.collection_alerts_aux].count_documents(
                {"_id": object_id}, limit=1
            )
            == 0
        ):
            with timer(
                f"Cross-match of {object_id} {candid}", alert_worker.verbose > 1
            ):
                xmatches = alert_worker.alert_filter__xmatch(alert)

            # Crossmatch new alert with most recent ZTF_alerts and insert
            with timer(
                f"ZTF Cross-match of {object_id} {candid}", alert_worker.verbose > 1
            ):
                xmatches = {
                    **xmatches,
                    **alert_worker.alert_filter__xmatch_ztf_alerts(alert),
                }

            alert_aux = {
                "_id": object_id,
                "cross_matches": xmatches,
                "prv_candidates": prv_candidates,
            }

            with timer(f"Aux ingesting {object_id} {candid}", alert_worker.verbose > 1):
                alert_worker.mongo.insert_one(
                    collection=alert_worker.collection_alerts_aux, document=alert_aux
                )

        else:
            with timer(
                f"Aux updating of {object_id} {candid}", alert_worker.verbose > 1
            ):
                alert_worker.mongo.db[alert_worker.collection_alerts_aux].update_one(
                    {"_id": object_id},
                    {"$addToSet": {"prv_candidates": {"$each": prv_candidates}}},
                    upsert=True,
                )

            # Crossmatch exisiting alert with most recent record in ZTF_alerts and update aux
            with timer(
                f"ZTF Cross-match of {object_id} {candid}", alert_worker.verbose > 1
            ):
                xmatches_ztf = alert_worker.alert_filter__xmatch_ztf_alerts(alert)

            with timer(
                f"Aux updating of {object_id} {candid}", alert_worker.verbose > 1
            ):
                alert_worker.mongo.db[alert_worker.collection_alerts_aux].update_one(
                    {"_id": object_id},
                    {"$set": {"ZTF_alerts": xmatches_ztf}},
                    upsert=True,
                )

        if config["misc"]["broker"]:
            # execute user-defined alert filters
            with timer(f"Filtering of {object_id} {candid}", alert_worker.verbose > 1):
                passed_filters = alert_worker.alert_filter__user_defined(
                    alert_worker.filter_templates, alert
                )
            if alert_worker.verbose > 1:
                log(
                    f"{object_id} {candid} number of filters passed: {len(passed_filters)}"
                )

            # post to SkyPortal
            alert_worker.alert_sentinel_skyportal(alert, prv_candidates, passed_filters)

        # clean up after thyself
        del (
            alert,
            prv_candidates,
            xmatches,
            xmatches_ztf,
            alert_aux,
            passed_filters,
            candid,
            object_id,
        )


class TURBOAlertWorker(AlertWorker, ABC):
    def __init__(self, **kwargs):
        super().__init__(instrument="TURBO", **kwargs)

        # talking to SkyPortal?
        if not config["misc"]["broker"]:
            return

        # get TURBO alert stream id on SP
        self.TURBO_stream_id = None
        with timer("Getting TURBO alert stream id from SkyPortal", self.verbose > 1):
            response = self.api_skyportal("GET", "/api/streams")
        if response.json()["status"] == "success" and len(response.json()["data"]) > 0:
            for stream in response.json()["data"]:
                if "TURBO" in stream.get("name"):
                    self.TURBO_stream_id = stream["id"]
        if self.TURBO_stream_id is None:
            log("Failed to get TURBO alert stream ids from SkyPortal")
            raise ValueError("Failed to get TURBO alert stream ids from SkyPortal")

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
        self.run_forever = True
        self.filter_monitor = threading.Thread(target=self.reload_filters)
        self.filter_monitor.start()

        log("Loaded user-defined filters:")
        log(self.filter_templates)

    def get_active_filters(self):
        """Fetch user-defined filters from own db marked as active."""
        return list(
            self.mongo.db[config["database"]["collections"]["filters"]].aggregate(
                [
                    {
                        "$match": {
                            "catalog": self.collection_alerts,
                            "active": True,
                        }
                    },
                    {
                        "$project": {
                            "group_id": 1,
                            "filter_id": 1,
                            "permissions": 1,
                            "autosave": 1,
                            "auto_followup": 1,
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
            try:
                # collect additional info from SkyPortal
                with timer(
                    f"Getting info on group id={active_filter['group_id']} from SkyPortal",
                    self.verbose > 1,
                ):
                    response = self.api_skyportal_get_group(active_filter["group_id"])
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
                pipeline = deepcopy(self.filter_pipeline_upstream) + bson_loads(
                    active_filter["fv"]["pipeline"]
                )

                # if autosave is a dict with a pipeline key, also add the upstream pipeline to it:
                if (
                    isinstance(active_filter.get("autosave", None), dict)
                    and active_filter.get("autosave", {}).get("pipeline", None)
                    is not None
                ):
                    active_filter["autosave"]["pipeline"] = deepcopy(
                        self.filter_pipeline_upstream
                    ) + bson_loads(active_filter["autosave"]["pipeline"])
                # same for the auto_followup pipeline:
                if (
                    isinstance(active_filter.get("auto_followup", None), dict)
                    and active_filter.get("auto_followup", {}).get("pipeline", None)
                    is not None
                ):
                    active_filter["auto_followup"]["pipeline"] = deepcopy(
                        self.filter_pipeline_upstream
                    ) + bson_loads(active_filter["auto_followup"]["pipeline"])

                filter_template = {
                    "group_id": active_filter["group_id"],
                    "filter_id": active_filter["filter_id"],
                    "group_name": group_name,
                    "filter_name": filter_name,
                    "fid": active_filter["fv"]["fid"],
                    "permissions": active_filter["permissions"],
                    "autosave": active_filter.get("autosave", False),
                    "auto_followup": active_filter.get("auto_followup", {}),
                    "update_annotations": active_filter.get(
                        "update_annotations", False
                    ),
                    "pipeline": deepcopy(pipeline),
                }

                filter_templates.append(filter_template)
            except Exception as e:
                log(
                    "Failed to generate filter template for "
                    f"group_id={active_filter['group_id']} filter_id={active_filter['filter_id']}: {e}"
                )
                continue

        return filter_templates

    def reload_filters(self):
        """
        Helper function to periodically reload filters from SkyPortal

        :return:
        """
        while self.run_forever:
            time.sleep(60 * 5)

            active_filters = self.get_active_filters()
            self.filter_templates = self.make_filter_templates(active_filters)

    def alert_filter__xmatch_ztf_alerts(
        self, alert: Mapping, ztf_stream: str = "ZTF_alerts"
    ) -> dict:
        """
        Run cross-match with ZTF alerts
        Only searches for the most recent entry to keep it efficient

        :param alert:
        :param ztf_stream: Name of ZTF alert stream catalog
        :return:
        """

        xmatches = dict()

        # cone search radius in arcsec:
        cone_search_radius_ztf = 8.0
        # convert arcsec to rad:
        PI = 3.141592653589793
        cone_search_radius_ztf *= PI / (180.0 * 3600)

        try:
            ra = float(alert["candidate"]["ra"])
            dec = float(alert["candidate"]["dec"])

            # geojson-friendly ra:
            ra_geojson = ra - 180.0
            dec_geojson = dec

            # restrict to public data only
            catalog_filter = {"candidate.programid": 1}
            catalog_sort = [("candidate.jd", -1)]
            catalog_projection = {
                "objectId": 1,
                "candid": 1,
                "candidate.jd": 1,
                "candidate.magpsf": 1,
                "candidate.sigmapsf": 1,
                "candidate.fid": 1,
                "candidate.drb": 1,
                "coordinates.radec_str": 1,
            }

            # do search of everything that is within the cross-match radius
            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra_geojson, dec_geojson],
                        cone_search_radius_ztf,
                    ]
                }
            }
            # Just find the most recent alert within the crossmatch radius, if any
            latest_alert = list(
                self.mongo.db[ztf_stream].find(
                    {**object_position_query, **catalog_filter},
                    projection={**catalog_projection},
                    sort=catalog_sort,
                    limit=1,
                )
            )

            # Check if any result was found (latest_alert is not null)
            xmatches[ztf_stream] = latest_alert if latest_alert else []

        except Exception as e:
            log(str(e))

        return xmatches

    def alert_put_photometry(self, alert):
        """PUT photometry to SkyPortal

        :param alert:
        :return:
        """
        with timer(
            f"Making alert photometry of {alert['objectId']} {alert['candid']}",
            self.verbose > 1,
        ):
            df_photometry = self.make_photometry(alert)

        # post photometry
        photometry = {
            "obj_id": alert["objectId"],
            "stream_ids": [int(self.TURBO_stream_id)],
            "instrument_id": self.instrument_id,
            "mjd": df_photometry["mjd"].tolist(),
            "flux": df_photometry["flux"].tolist(),
            "fluxerr": df_photometry["fluxerr"].tolist(),
            "zp": df_photometry["zp"].tolist(),
            "magsys": df_photometry["zpsys"].tolist(),
            "filter": df_photometry["filter"].tolist(),
            "ra": df_photometry["ra"].tolist(),
            "dec": df_photometry["dec"].tolist(),
        }

        if (len(photometry.get("flux", ())) > 0) or (
            len(photometry.get("fluxerr", ())) > 0
        ):
            with timer(
                f"Posting photometry of {alert['objectId']} {alert['candid']}, "
                f"stream_id={self.TURBO_stream_id} to SkyPortal",
                self.verbose > 1,
            ):
                response = self.api_skyportal("PUT", "/api/photometry", photometry)
            if response.json()["status"] == "success":
                log(
                    f"Posted {alert['objectId']} photometry stream_id={self.TURBO_stream_id} to SkyPortal"
                )
            else:
                log(
                    f"Failed to post {alert['objectId']} photometry stream_id={self.TURBO_stream_id} to SkyPortal"
                )
            log(response.json())


class WorkerInitializer(dask.distributed.WorkerPlugin):
    def __init__(self, *args, **kwargs):
        self.alert_worker = None

    def setup(self, worker: dask.distributed.Worker):
        self.alert_worker = TURBOAlertWorker()


def topic_listener(
    topic,
    bootstrap_servers: str,
    offset_reset: str = "earliest",
    group: str = None,
    test: bool = False,
):
    """
        Listen to a Kafka topic with TURBO alerts
    :param topic:
    :param bootstrap_servers:
    :param offset_reset:
    :param group:
    :param test: when testing, terminate once reached end of partition
    :return:
    """

    # Configure dask client
    dask_client = dask.distributed.Client(
        address=f"{config['dask_turbo']['host']}:{config['dask_turbo']['scheduler_port']}"
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
    stream_reader = TURBOAlertConsumer(topic, dask_client, instrument="TURBO", **conf)

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

            if obs_date is None:
                datestr = datetime.datetime.utcnow().strftime("%Y%m%d")
            else:
                datestr = obs_date

            # get kafka topic names with kafka-topics command
            if not test:
                # Production Kafka stream at IPAC

                # TODO: Verify naming convention
                topics_tonight = [f"turbo_{datestr}"]
            else:
                # Local test stream
                kafka_cmd = [
                    os.path.join(config["kafka"]["path"], "bin", "kafka-topics.sh"),
                    "--bootstrap-server",
                    config["kafka"]["bootstrap.test.servers"],
                    "-list",
                ]

                topics = (
                    subprocess.run(kafka_cmd, stdout=subprocess.PIPE)
                    .stdout.decode("utf-8")
                    .split("\n")[:-1]
                )

                topics_tonight = [
                    t for t in topics if (datestr in t) and ("turbo" in t)
                ]

            log(f"turbo: Topics tonight: {topics_tonight}")

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
                    log(f"set daemon to true {topics_on_watch}")
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
                for t in topics_on_watch:
                    topics_on_watch[t].kill()
                break

        except Exception as e:
            log(str(e))
            _err = traceback.format_exc()
            log(str(_err))

        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kowalski's TURBO Alert Broker")
    parser.add_argument("--obsdate", help="observing date YYYYMMDD")
    parser.add_argument("--test", help="listen to the test stream", action="store_true")

    args = parser.parse_args()

    watchdog(obs_date=args.obsdate, test=args.test)
