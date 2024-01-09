import argparse
import datetime
import multiprocessing
import os
import subprocess
import sys
import threading
import time
import traceback
import numpy as np
from abc import ABC
from copy import deepcopy
from typing import Mapping, Sequence

import dask.distributed
from kowalski.alert_brokers.alert_broker import AlertConsumer, AlertWorker, EopError
from bson.json_util import loads as bson_loads
from kowalski.utils import init_db_sync, timer, retry
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


class ZTFAlertConsumer(AlertConsumer, ABC):
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
        alert_worker: ZTFAlertWorker = worker.plugins["worker-init"].alert_worker

        log(f"{topic} {object_id} {candid} {worker.address}")

        # return if this alert packet has already been processed and ingested into collection_alerts:
        if (
            retry(
                alert_worker.mongo.db[alert_worker.collection_alerts].count_documents
            )({"candid": candid}, limit=1)
            == 1
        ):
            return

        # candid not in db, ingest decoded avro packet into db
        with timer(f"Mongification of {object_id} {candid}", alert_worker.verbose > 1):
            alert, prv_candidates, fp_hists = alert_worker.alert_mongify(alert)

        # create alert history
        all_prv_candidates = deepcopy(prv_candidates) + [deepcopy(alert["candidate"])]
        with timer(
            f"Gather all previous candidates for {object_id} {candid}",
            alert_worker.verbose > 1,
        ):
            # get all prv_candidates for this objectId:
            existing_aux = retry(
                alert_worker.mongo.db[alert_worker.collection_alerts_aux].find_one
            )({"_id": object_id}, {"prv_candidates": 1})
            if (
                existing_aux is not None
                and len(existing_aux.get("prv_candidates", [])) > 0
            ):
                all_prv_candidates += existing_aux["prv_candidates"]

            # get all alerts for this objectId:
            existing_alerts = list(
                alert_worker.mongo.db[alert_worker.collection_alerts].find(
                    {"objectId": object_id}, {"candidate": 1}
                )
            )
            if len(existing_alerts) > 0:
                all_prv_candidates += [
                    existing_alert["candidate"] for existing_alert in existing_alerts
                ]
            del existing_aux, existing_alerts

        # ML models:
        with timer(f"MLing of {object_id} {candid}", alert_worker.verbose > 1):
            scores = alert_worker.alert_filter__ml(alert, all_prv_candidates)
            alert["classifications"] = scores

        with timer(f"Ingesting {object_id} {candid}", alert_worker.verbose > 1):
            retry(alert_worker.mongo.insert_one)(
                collection=alert_worker.collection_alerts, document=alert
            )

        # prv_candidates: pop nulls - save space
        prv_candidates = [
            {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
            for prv_candidate in prv_candidates
        ]

        # fp_hists: pop nulls - save space
        fp_hists = [
            {kk: vv for kk, vv in fp_hist.items() if vv not in [None, -99999, -99999.0]}
            for fp_hist in fp_hists
        ]

        alert_aux, xmatches, passed_filters = None, None, None
        # cross-match with external catalogs if objectId not in collection_alerts_aux:
        if (
            retry(
                alert_worker.mongo.db[
                    alert_worker.collection_alerts_aux
                ].count_documents
            )({"_id": object_id}, limit=1)
            == 0
        ):
            with timer(
                f"Cross-match of {object_id} {candid}", alert_worker.verbose > 1
            ):
                xmatches = alert_worker.alert_filter__xmatch(alert)

            alert_aux = {
                "_id": object_id,
                "cross_matches": xmatches,
                "prv_candidates": prv_candidates,
            }

            # only add the fp_hists if its a brand new object, not just if there is no entry there
            if alert["candidate"]["ndethist"] <= 1:
                alert_aux["fp_hists"] = alert_worker.format_fp_hists(alert, fp_hists)

            with timer(f"Aux ingesting {object_id} {candid}", alert_worker.verbose > 1):
                retry(alert_worker.mongo.insert_one)(
                    collection=alert_worker.collection_alerts_aux, document=alert_aux
                )

        else:
            with timer(
                f"Aux updating of {object_id} {candid}", alert_worker.verbose > 1
            ):
                # update prv_candidates
                retry(
                    alert_worker.mongo.db[alert_worker.collection_alerts_aux].update_one
                )(
                    {"_id": object_id},
                    {"$addToSet": {"prv_candidates": {"$each": prv_candidates}}},
                    upsert=True,
                )

                # if there is no fp_hists for this object, we don't update anything
                # the idea is that we start accumulating FP only for new objects, to avoid
                # having some objects with incomplete FP history, which would be confusing for the filters
                # either there is full FP, or there isn't any
                if (
                    retry(
                        alert_worker.mongo.db[
                            alert_worker.collection_alerts_aux
                        ].count_documents
                    )(
                        {"_id": alert["objectId"], "fp_hists": {"$exists": True}},
                        limit=1,
                    )
                    == 1
                ):
                    alert_worker.update_fp_hists(
                        alert, alert_worker.format_fp_hists(alert, fp_hists)
                    )

        if config["misc"]["broker"]:
            # execute user-defined alert filters
            with timer(f"Filtering of {object_id} {candid}", alert_worker.verbose > 1):
                passed_filters = alert_worker.alert_filter__user_defined(
                    alert_worker.filter_templates, alert, all_prv_candidates
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
            fp_hists,
            all_prv_candidates,
            scores,
            xmatches,
            alert_aux,
            passed_filters,
            candid,
            object_id,
        )

        return


class ZTFAlertWorker(AlertWorker, ABC):
    def __init__(self, **kwargs):
        super().__init__(instrument="ZTF", **kwargs)

        # talking to SkyPortal?
        if not config["misc"]["broker"]:
            return

        # get ZTF alert stream ids to program ids mapping
        self.ztf_program_id_to_stream_id = dict()
        with timer("Getting ZTF alert stream ids from SkyPortal", self.verbose > 1):
            response = self.api_skyportal("GET", "/api/streams")
        if response.json()["status"] == "success" and len(response.json()["data"]) > 0:
            for stream in response.json()["data"]:
                if stream.get("name") == "ZTF Public":
                    self.ztf_program_id_to_stream_id[1] = stream["id"]
                if stream.get("name") == "ZTF Public+Partnership":
                    self.ztf_program_id_to_stream_id[2] = stream["id"]
                if stream.get("name") == "ZTF Public+Partnership+Caltech":
                    # programid=0 is engineering data
                    self.ztf_program_id_to_stream_id[0] = stream["id"]
                    self.ztf_program_id_to_stream_id[3] = stream["id"]
            if len(self.ztf_program_id_to_stream_id) != 4:
                log("Failed to map ZTF alert stream ids from SkyPortal to program ids")
                raise ValueError(
                    "Failed to map ZTF alert stream ids from SkyPortal to program ids"
                )
            log(
                f"Got ZTF program id to SP stream id mapping: {self.ztf_program_id_to_stream_id}"
            )
        else:
            log("Failed to get ZTF alert stream ids from SkyPortal")
            raise ValueError("Failed to get ZTF alert stream ids from SkyPortal")

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
        # todo: query SP to make sure the filters still exist there and we're not out of sync;
        #       clean up if necessary
        return list(
            retry(
                self.mongo.db[config["database"]["collections"]["filters"]].aggregate
            )(
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
                # set permissions
                pipeline[0]["$match"]["candidate.programid"]["$in"] = active_filter[
                    "permissions"
                ]
                pipeline[3]["$project"]["prv_candidates"]["$filter"]["cond"]["$and"][0][
                    "$in"
                ][1] = active_filter["permissions"]

                # if autosave is a dict with a pipeline key, also add the upstream pipeline to it:
                if (
                    isinstance(active_filter.get("autosave", None), dict)
                    and active_filter.get("autosave", {}).get("pipeline", None)
                    is not None
                ):
                    active_filter["autosave"]["pipeline"] = deepcopy(
                        self.filter_pipeline_upstream
                    ) + bson_loads(active_filter["autosave"]["pipeline"])
                    # set permissions
                    active_filter["autosave"]["pipeline"][0]["$match"][
                        "candidate.programid"
                    ]["$in"] = active_filter["permissions"]
                    active_filter["autosave"]["pipeline"][3]["$project"][
                        "prv_candidates"
                    ]["$filter"]["cond"]["$and"][0]["$in"][1] = active_filter[
                        "permissions"
                    ]
                # same for the auto_followup pipeline:
                if (
                    isinstance(active_filter.get("auto_followup", None), dict)
                    and active_filter.get("auto_followup", {}).get("pipeline", None)
                    is not None
                ):
                    active_filter["auto_followup"]["pipeline"] = deepcopy(
                        self.filter_pipeline_upstream
                    ) + bson_loads(active_filter["auto_followup"]["pipeline"])
                    # set permissions
                    active_filter["auto_followup"]["pipeline"][0]["$match"][
                        "candidate.programid"
                    ]["$in"] = active_filter["permissions"]
                    active_filter["auto_followup"]["pipeline"][3]["$project"][
                        "prv_candidates"
                    ]["$filter"]["cond"]["$and"][0]["$in"][1] = active_filter[
                        "permissions"
                    ]

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

            log(
                f"Alert {alert['objectId']} contains program_ids={df_photometry['programid']}"
            )

            df_photometry["stream_id"] = df_photometry["programid"].apply(
                lambda programid: self.ztf_program_id_to_stream_id[programid]
            )

        log(
            f"Posting {alert['objectId']} photometry for stream_ids={set(df_photometry.stream_id.unique())} to SkyPortal"
        )

        # post photometry by stream_id
        for stream_id in set(df_photometry.stream_id.unique()):
            stream_id_mask = df_photometry.stream_id == int(stream_id)
            photometry = {
                "obj_id": alert["objectId"],
                "stream_ids": [int(stream_id)],
                "instrument_id": self.instrument_id,
                "mjd": df_photometry.loc[stream_id_mask, "mjd"].tolist(),
                "flux": df_photometry.loc[stream_id_mask, "flux"].tolist(),
                "fluxerr": df_photometry.loc[stream_id_mask, "fluxerr"].tolist(),
                "zp": df_photometry.loc[stream_id_mask, "zp"].tolist(),
                "magsys": df_photometry.loc[stream_id_mask, "zpsys"].tolist(),
                "filter": df_photometry.loc[stream_id_mask, "filter"].tolist(),
                "ra": df_photometry.loc[stream_id_mask, "ra"].tolist(),
                "dec": df_photometry.loc[stream_id_mask, "dec"].tolist(),
            }

            if (len(photometry.get("flux", ())) > 0) or (
                len(photometry.get("fluxerr", ())) > 0
            ):
                with timer(
                    f"Posting photometry of {alert['objectId']} {alert['candid']}, "
                    f"stream_id={stream_id} to SkyPortal",
                    self.verbose > 1,
                ):
                    try:
                        response = self.api_skyportal(
                            "PUT", "/api/photometry", photometry, timeout=15
                        )
                        if response.json()["status"] == "success":
                            log(
                                f"Posted {alert['objectId']} photometry stream_id={stream_id} to SkyPortal"
                            )
                        else:
                            raise ValueError(response.json()["message"])
                    except Exception as e:
                        log(
                            f"Failed to post {alert['objectId']} photometry stream_id={stream_id} to SkyPortal: {e}"
                        )
                        continue

    def flux_to_mag(self, flux, fluxerr, zp):
        """Convert flux to magnitude and calculate SNR

        :param flux:
        :param fluxerr:
        :param zp:
        :param snr_threshold:
        :return:
        """
        # make sure its all numpy floats or nans
        values = np.array([flux, fluxerr, zp], dtype=np.float64)
        snr = values[0] / values[1]
        mag = -2.5 * np.log10(values[0]) + values[2]
        magerr = 1.0857 * (values[1] / values[0])
        limmag3sig = -2.5 * np.log10(3 * values[1]) + values[2]
        limmag5sig = -2.5 * np.log10(5 * values[1]) + values[2]
        if np.isnan(snr):
            return {}
        if snr < 0:
            return {
                "snr": snr,
            }
        mag_data = {
            "mag": mag,
            "magerr": magerr,
            "snr": snr,
            "limmag3sig": limmag3sig,
            "limmag5sig": limmag5sig,
        }
        # remove all NaNs fields
        mag_data = {k: v for k, v in mag_data.items() if not np.isnan(v)}
        return mag_data

    def format_fp_hists(self, alert, fp_hists):
        if len(fp_hists) == 0:
            return []
        # sort by jd
        fp_hists = sorted(fp_hists, key=lambda x: x["jd"])

        # add the "alert_mag" field to the new fp_hist
        # as well as alert_ra, alert_dec
        for i, fp in enumerate(fp_hists):
            fp_hists[i] = {
                **fp,
                **self.flux_to_mag(
                    flux=fp.get("forcediffimflux", np.nan),
                    fluxerr=fp.get("forcediffimfluxunc", np.nan),
                    zp=fp["magzpsci"],
                ),
                "alert_mag": alert["candidate"]["magpsf"],
                "alert_ra": alert["candidate"]["ra"],
                "alert_dec": alert["candidate"]["dec"],
            }

        return fp_hists

    def update_fp_hists(self, alert, formatted_fp_hists):
        # update the existing fp_hist with the new one
        # instead of treating it as a set,
        # if some entries have the same jd, keep the one with the highest alert_mag

        # make sure this is an aggregate pipeline in mongodb
        if len(formatted_fp_hists) == 0:
            return

        with timer(
            f"Updating fp_hists of {alert['objectId']} {alert['candid']}",
            self.verbose > 1,
        ):
            update_pipeline = [
                # 0. match the document
                {"$match": {"_id": alert["objectId"]}},
                # 1. concat the new fp_hists with the existing ones
                {
                    "$project": {
                        "all_fp_hists": {
                            "$concatArrays": [
                                {"$ifNull": ["$fp_hists", []]},
                                formatted_fp_hists,
                            ]
                        }
                    }
                },
                # 2. unwind the resulting array to get one document per fp_hist
                {"$unwind": "$all_fp_hists"},
                # 3. group by jd and keep the one with the highest alert_mag for each jd
                {
                    "$set": {
                        "all_fp_hists.alert_mag": {
                            "$cond": {
                                "if": {
                                    "$eq": [
                                        {"$type": "$all_fp_hists.alert_mag"},
                                        "missing",
                                    ]
                                },
                                "then": -99999.0,
                                "else": "$all_fp_hists.alert_mag",
                            }
                        }
                    }
                },
                # 4. sort by jd and alert_mag
                {
                    "$sort": {
                        "all_fp_hists.jd": 1,
                        "all_fp_hists.alert_mag": 1,
                    }
                },
                # 5. group all the deduplicated fp_hists back into an array, keeping the first one (the brightest at each jd)
                {
                    "$group": {
                        "_id": "$all_fp_hists.jd",
                        "fp_hist": {"$first": "$$ROOT.all_fp_hists"},
                    }
                },
                # 6. sort by jd again
                {"$sort": {"fp_hist.jd": 1}},
                # 7. group all the fp_hists documents back into a single array
                {"$group": {"_id": None, "fp_hists": {"$push": "$fp_hist"}}},
                # 8. project only the new fp_hists array
                {"$project": {"fp_hists": 1, "_id": 0}},
            ]
            n_retries = 0
            while True:
                # run the pipeline and then update the document
                new_fp_hists = (
                    self.mongo.db[self.collection_alerts_aux]
                    .aggregate(
                        update_pipeline,
                        allowDiskUse=True,
                    )
                    .next()
                    .get("fp_hists", [])
                )

                # update the document, only if there is still less points in the DB than in the new fp_hists.
                # Otherwise, rerun the pipeline. This is to help a little bit with concurrency issues
                result = self.mongo.db[self.collection_alerts_aux].find_one_and_update(
                    {
                        "_id": alert["objectId"],
                        f"fp_hists.{len(new_fp_hists)}": {"$exists": False},
                    },
                    {"$set": {"fp_hists": new_fp_hists}},
                )
                if result is None:
                    n_retries += 1
                    if n_retries > 10:
                        log(
                            f"Failed to update fp_hists of {alert['objectId']} {alert['candid']}"
                        )
                        break
                    else:
                        log(
                            f"Retrying to update fp_hists of {alert['objectId']} {alert['candid']}"
                        )
                        time.sleep(1)
                else:
                    break


class WorkerInitializer(dask.distributed.WorkerPlugin):
    def __init__(self, *args, **kwargs):
        self.alert_worker = None

    def setup(self, worker: dask.distributed.Worker):
        self.alert_worker = ZTFAlertWorker()


def topic_listener(
    topic,
    bootstrap_servers: str,
    offset_reset: str = "earliest",
    group: str = None,
    test: bool = False,
):
    """
        Listen to a Kafka topic with ZTF alerts
    :param topic:
    :param bootstrap_servers:
    :param offset_reset:
    :param group:
    :param test: when testing, terminate once reached end of partition
    :return:
    """

    os.environ["MALLOC_TRIM_THRESHOLD_"] = "65536"
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
    stream_reader = ZTFAlertConsumer(topic, dask_client, instrument="ZTF", **conf)

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

                # as of 20180403, the naming convention is ztf_%Y%m%d_programidN
                topics_tonight = []
                for stream in [1, 2, 3]:
                    topics_tonight.append(f"ztf_{datestr}_programid{stream}")

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
                    t
                    for t in topics
                    if (datestr in t)
                    and ("programid" in t)
                    and ("zuds" not in t)
                    and ("pgir" not in t)
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
    parser = argparse.ArgumentParser(description="Kowalski's ZTF Alert Broker")
    parser.add_argument("--obsdate", help="observing date YYYYMMDD")
    parser.add_argument("--test", help="listen to the test stream", action="store_true")

    args = parser.parse_args()

    watchdog(obs_date=args.obsdate, test=args.test)
