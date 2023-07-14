import ctypes
import platform
import time

from dask.distributed import LocalCluster

from kowalski.alert_brokers.alert_broker_ztf import WorkerInitializer  # noqa: F401
from kowalski.config import load_config
from kowalski.log import log


def trim_memory() -> int:
    if platform.uname()[0] != "Darwin":
        libc = ctypes.CDLL("libc.so.6")
        return libc.malloc_trim(0)


""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


if __name__ == "__main__":

    cluster = LocalCluster(
        threads_per_worker=config["dask"]["threads_per_worker"],
        n_workers=config["dask"]["n_workers"],
        scheduler_port=config["dask"]["scheduler_port"],
        dashboard_address=config["dask"]["dashboard_address"],
        lifetime=config["dask"]["lifetime"],
        lifetime_stagger=config["dask"]["lifetime_stagger"],
        lifetime_restart=config["dask"]["lifetime_restart"],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
        client = cluster.get_client()
        client.run(trim_memory)
