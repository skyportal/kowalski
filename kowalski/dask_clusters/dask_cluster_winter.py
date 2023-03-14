import time

from dask.distributed import LocalCluster
from kowalski.utils import log
from kowalski.config import load_config
from kowalski.alert_brokers.alert_broker_winter import WorkerInitializer  # noqa: F401

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


if __name__ == "__main__":
    if "dask_wntr" not in config.keys():
        log(
            "dask_wntr not found in config file, please update the config file accordingly."
        )
        exit(1)

    cluster = LocalCluster(
        threads_per_worker=config["dask_wntr"]["threads_per_worker"],
        n_workers=config["dask_wntr"]["n_workers"],
        scheduler_port=config["dask_wntr"]["scheduler_port"],
        dashboard_address=config["dask_wntr"]["dashboard_address"],
        lifetime=config["dask_wntr"]["lifetime"],
        lifetime_stagger=config["dask_wntr"]["lifetime_stagger"],
        lifetime_restart=config["dask_wntr"]["lifetime_restart"],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
