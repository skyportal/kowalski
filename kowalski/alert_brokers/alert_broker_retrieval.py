import argparse
import datetime
import multiprocessing
import os
import subprocess
import sys
import time
import traceback
from abc import ABC
from typing import Mapping

import dask.distributed
from kowalski.alert_brokers.alert_broker import AlertConsumer, AlertWorker, EopError
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
        alert_worker = worker.plugins["worker-init"].alert_worker

        log(f"{topic} {object_id} {candid} {worker.address}")

        # candid not in db, ingest decoded avro packet into db
        with timer(f"Mongification of {object_id} {candid}", alert_worker.verbose > 1):
            alert, prv_candidates = alert_worker.alert_mongify(alert)

        # prv_candidates: pop nulls - save space
        prv_candidates = [
            {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
            for prv_candidate in prv_candidates
        ]

        alert_aux, xmatches = None, None
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

            with timer(f"Aux ingesting {object_id} {candid}", alert_worker.verbose > 1):
                retry(alert_worker.mongo.insert_one)(
                    collection=alert_worker.collection_alerts_aux, document=alert_aux
                )

        else:
            with timer(
                f"Aux updating of {object_id} {candid}", alert_worker.verbose > 1
            ):
                retry(
                    alert_worker.mongo.db[alert_worker.collection_alerts_aux].update_one
                )(
                    {"_id": object_id},
                    {
                        "$addToSet": {
                            "prv_candidates": {"$each": prv_candidates},
                        }
                    },
                    upsert=True,
                )

        # clean up after thyself
        del (
            alert,
            prv_candidates,
            xmatches,
            alert_aux,
            candid,
            object_id,
        )

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
                    self.submit_alert(record)

                # clean up after thyself
                del msg_decoded

            except Exception as e:
                print("Error in poll!")
                log(e)
                _err = traceback.format_exc()
                log(_err)

        # clean up after thyself
        del msg


class ZTFRetrievalAlertWorker(AlertWorker, ABC):
    def __init__(self, **kwargs):
        super().__init__(instrument="ZTF", **kwargs)

        # talking to SkyPortal?
        if not config["misc"]["broker"]:
            return

        # filter pipeline upstream: select current alert, ditch cutouts, and merge with aux data
        # including archival photometry and cross-matches:
        self.filter_pipeline_upstream = config["database"]["filters"][
            self.collection_alerts
        ]
        log("Upstream filtering pipeline:")
        log(self.filter_pipeline_upstream)

        # set up watchdog for periodic refresh of the filter templates, in case those change
        self.run_forever = True


class WorkerInitializer(dask.distributed.WorkerPlugin):
    def __init__(self, *args, **kwargs):
        self.alert_worker = None

    def setup(self, worker: dask.distributed.Worker):
        self.alert_worker = ZTFRetrievalAlertWorker()


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
        address=f"{config['dask_retrieval']['host']}:{config['dask_retrieval']['scheduler_port']}"
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

    def is_night_pst() -> bool:
        now_utc = datetime.datetime.utcnow()
        return now_utc.hour >= 0 and now_utc.hour < 14

    counter = 0
    while True:
        if counter % 1000 == 0:
            counter = 0
            if is_night_pst():
                log("Night time, pausing retrieval until morning")
                time.sleep(120)
            else:
                log("Day time, retrieving alerts")
        try:
            # poll!
            stream_reader.poll()
            counter += 1

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
