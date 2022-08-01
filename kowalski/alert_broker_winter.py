from abc import ABC
import argparse
from bson.json_util import loads as bson_loads
from copy import deepcopy
import dask.distributed
import datetime
import multiprocessing
import os
import subprocess
import sys
import time
import traceback
import threading
from typing import Mapping, Sequence

from alert_broker import (
    AlertConsumer,
    AlertWorker,
    EopError
)
from utils import init_db_sync, load_config, log, timer


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]

# Misc: not in alert_broker_pgir
# TODO: remove as not needed

import logging

logger = logging.getLogger(__name__)  
logger.setLevel(logging.INFO)


class WNTRAlertConsumer(AlertConsumer, ABC):
    """
    Creates an alert stream Kafka consumer for a given topic,
    reads incoming packets, and ingests stream into database
    based on applied filters. 

    """
   
    def __init__(self, topic:str, dask_client: dask.distributed.Client, **kwargs):
        """
        Initializes Kafka consumer.

        :param bootstrap_server_str: IP addresses of brokers to subscribe to, comma-separated
        e.g. 192.168.0.64:9092,192.168.0.65:9092,192.168.0.66:9092
        :type bootstrap_server_str: str
        :param topic: name of topic to poll from, e.g. winter_20220728
        :type topic: str
        :param group_id: id, typically prefix of topic name, e.g. winter
        :type group_id: str
        :param verbose: _description_, defaults to 2
        :type verbose: int, optional
        """
        super().__init__(topic, dask_client, **kwargs)
    
    @staticmethod
    def process_alert(alert: Mapping, topic: str):
        """
        Main function that runs on a single alert.
            -Read top-level packet field
            -Separate alert and prv_candidate in MongoDB prep

        :param alert: decoded alert from Kafka stream
        :param topic: Kafka stream topic name for bookkeeping
        :return:
        """
        print(f'In process alert')
        candid = alert["candid"]
        object_id = alert["objectId"]

        candidate = alert["candidate"]
        print(f"{topic} {object_id} {candid} in process_alert")
        print(f"----------------------------------------------")
        print(f"{candidate}")
        print(f"----------------------------------------------")

        # get worker running current task
        worker = dask.distributed.get_worker()
        alert_worker = worker.plugins["worker-init"].alert_worker

        log(f"{topic} {object_id} {candid} {worker.address}")

        # TODO check: return if this alert packet has already been processed 
        # and ingested into collection_alerts:

        # candid not in db, ingest decoded avro packet into db
        with timer(f"Mongification of {object_id} {candid}"):
            print(f'Trying to mongify')
            alert, prv_candidates = alert_worker.alert_mongify(alert)

        # prv_candidates: pop nulls - save space
        prv_candidates = [
            {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
            for prv_candidate in prv_candidates
        ]

class WNTRAlertWorker(AlertWorker, ABC):
    def __init__(self, **kwargs):
        super().__init__(instrument="WNTR", **kwargs)
        
class WorkerInitializer(dask.distributed.WorkerPlugin):
    def __init__(self, *args, **kwargs):
        self.alert_worker = None

    def setup(self, worker: dask.distributed.Worker):
        self.alert_worker = WNTRAlertWorker()

def topic_listener(
    topic,
    bootstrap_servers: str,
    offset_reset: str = "earliest",
    group: str = None,
    test: bool = False,
):
    """
        Listen to a Kafka topic with WNTR alerts
    :param topic:
    :param bootstrap_servers:
    :param offset_reset:
    :param group:
    :param test: when testing, terminate once reached end of partition
    :return:
    """
    # Configure dask client
    dask_client = dask.distributed.Client(
        address=f"{config['dask_wntr']['host']}:{config['dask_wntr']['scheduler_port']}"
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
    # TODO change instrument to WNTR
    stream_reader = WNTRAlertConsumer(topic, dask_client, instrument="WNTR", **conf)

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
                print(f"in kafka_cmd")
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
            print(f"topics: {topics}")

            if obs_date is None:
                datestr = datetime.datetime.utcnow().strftime("%Y%m%d")
            else:
                datestr = obs_date
            # as of 20220801, the naming convention is winter_%Y%m%d
            topics_tonight = [t for t in topics if (datestr in t) and ("winter" in t)]
            log(f"Topics tonight: {topics_tonight}")

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
                for t in topics_on_watch:
                    topics_on_watch[t].kill()
                break

        except Exception as e:
            log(str(e))
            _err = traceback.format_exc()
            log(str(_err))

        time.sleep(60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kowalski's WNTR Alert Broker")
    parser.add_argument("--obsdate", help="observing date YYYYMMDD")
    parser.add_argument("--test", help="listen to the test stream", action="store_true")

    args = parser.parse_args()

    watchdog(obs_date=args.obsdate, test=args.test)