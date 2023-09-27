import ctypes
import platform
import time

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


if __name__ == "__main__":

    cluster = LocalCluster(
        threads_per_worker=config["dask_retrieval"]["threads_per_worker"],
        n_workers=config["dask_retrieval"]["n_workers"],
        scheduler_port=config["dask_retrieval"]["scheduler_port"],
        dashboard_address=config["dask_retrieval"]["dashboard_address"],
        lifetime=config["dask_retrieval"]["lifetime"],
        lifetime_stagger=config["dask_retrieval"]["lifetime_stagger"],
        lifetime_restart=config["dask_retrieval"]["lifetime_restart"],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
        client = cluster.get_client()
        client.run(trim_memory)
