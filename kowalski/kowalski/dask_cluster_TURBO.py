from dask.distributed import LocalCluster
import time

from utils import load_config, log


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


if __name__ == "__main__":

    cluster = LocalCluster(
        threads_per_worker=config["dask_TURBO"]["threads_per_worker"],
        n_workers=config["dask_TURBO"]["n_workers"],
        scheduler_port=config["dask_TURBO"]["scheduler_port"],
        dashboard_address=config["dask_TURBO"]["dashboard_address"],
        lifetime=config["dask_TURBO"]["lifetime"],
        lifetime_stagger=config["dask_TURBO"]["lifetime_stagger"],
        lifetime_restart=config["dask_TURBO"]["lifetime_restart"],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
