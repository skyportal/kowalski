import time

from dask.distributed import LocalCluster
from kowalski.utils import load_config, log
from kowalski.alert_brokers.alert_broker_ztf import WorkerInitializer  # noqa: F401

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
