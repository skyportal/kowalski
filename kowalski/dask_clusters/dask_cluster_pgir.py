import time

from dask.distributed import LocalCluster
from kowalski.config import load_config
from kowalski.log import log
from kowalski.alert_brokers.alert_broker_pgir import WorkerInitializer  # noqa: F401

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


if __name__ == "__main__":

    cluster = LocalCluster(
        threads_per_worker=config["dask_pgir"]["threads_per_worker"],
        n_workers=config["dask_pgir"]["n_workers"],
        scheduler_port=config["dask_pgir"]["scheduler_port"],
        dashboard_address=config["dask_pgir"]["dashboard_address"],
        lifetime=config["dask_pgir"]["lifetime"],
        lifetime_stagger=config["dask_pgir"]["lifetime_stagger"],
        lifetime_restart=config["dask_pgir"]["lifetime_restart"],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
