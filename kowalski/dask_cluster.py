import os
from dask.distributed import LocalCluster
import time

from utils import load_config, log


""" load config and secrets """
KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/app")
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]


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
