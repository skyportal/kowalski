from dask.distributed import LocalCluster
import time

from utils import load_config, log


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


if __name__ == "__main__":

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
