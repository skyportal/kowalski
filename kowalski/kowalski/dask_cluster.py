from dask.distributed import LocalCluster
import time

from utils import load_config, log


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


if __name__ == "__main__":

    cluster = LocalCluster(
        threads_per_worker=config["dask"]["threads_per_worker"],
        n_workers=config["dask"]["n_workers"],
        scheduler_port=config["dask"]["scheduler_port"],
        lifetime=config["dask"]["lifetime"],
        lifetime_stagger=config["dask"]["lifetime_stagger"],
        lifetime_restart=config["dask"]["lifetime_restart"],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
