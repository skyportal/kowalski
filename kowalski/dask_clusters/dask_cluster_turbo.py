import time

from dask.distributed import LocalCluster
from kowalski.utils import load_config, log


""" load config and secrets """

config = load_config(config_file="config.yaml")["kowalski"]


if __name__ == "__main__":

    cluster = LocalCluster(
        threads_per_worker=config["dask_turbo"]["threads_per_worker"],
        n_workers=config["dask_turbo"]["n_workers"],
        scheduler_port=config["dask_turbo"]["scheduler_port"],
        dashboard_address=config["dask_turbo"]["dashboard_address"],
        lifetime=config["dask_turbo"]["lifetime"],
        lifetime_stagger=config["dask_turbo"]["lifetime_stagger"],
        lifetime_restart=config["dask_turbo"]["lifetime_restart"],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
