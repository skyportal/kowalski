import ctypes
import platform
import time
from datetime import datetime

from dask.distributed import LocalCluster

from kowalski.alert_brokers.alert_broker_retrieval import (  # noqa
    WorkerInitializer,  # noqa
)  # noqa
from kowalski.config import load_config
from kowalski.log import log


def trim_memory() -> int:
    # suggested by: https://www.coiled.io/blog/tackling-unmanaged-memory-with-dask
    # to try to deal with accumulating unmanaged memory
    try:
        if platform.uname()[0] != "Darwin":
            libc = ctypes.CDLL("libc.so.6")
            return libc.malloc_trim(0)
        else:
            return 0
    except Exception as e:
        log(f"Exception while trimming memory: {str(e)}")
        return 0


""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


def is_night_pst() -> bool:
    now_utc = datetime.utcnow()
    return now_utc.hour >= 2 and now_utc.hour < 14


def is_day_pst() -> bool:
    now_utc = datetime.utcnow()
    return now_utc.hour < 2 or now_utc.hour >= 14


if __name__ == "__main__":
    nb_workers = config["dask_retrieval"]["n_workers"]

    if is_night_pst():
        log("Starting in night mode with only 1 worker")
    if is_day_pst():
        log(f"Starting in day mode with {nb_workers} workers")

    cluster = LocalCluster(
        threads_per_worker=config["dask_retrieval"]["threads_per_worker"],
        n_workers=1 if is_night_pst() else nb_workers,
        scheduler_port=config["dask_retrieval"]["scheduler_port"],
        dashboard_address=config["dask_retrieval"]["dashboard_address"],
        lifetime=config["dask_retrieval"]["lifetime"],
        lifetime_stagger=config["dask_retrieval"]["lifetime_stagger"],
        lifetime_restart=config["dask_retrieval"]["lifetime_restart"],
    )
    log(cluster)

    counter = 0
    while True:
        # if the UTC time is between 2am and 2pm, and the number of workers is reduce the number of worker to 1
        # so that the other cluster can take over
        now_utc = datetime.utcnow()
        if is_night_pst() and len(cluster.scheduler_info["workers"]) == nb_workers:
            cluster.scale(1)
            log("Reducing number of workers to 1 in the night time")
        elif is_day_pst() and len(cluster.scheduler_info["workers"]) == 1:
            cluster.scale(nb_workers)
            log(f"Increasing number of workers to {nb_workers} in the day time")

        time.sleep(60)
        log("Heartbeat")

        counter += 1
        if counter % 10 == 0:
            client = cluster.get_client()
            client.run(trim_memory)
            log("Trimming memory")
            counter = 0
