import time

from dask.distributed import LocalCluster
from kowalski.utils import load_config, log

""" load config and secrets """

config = load_config(config_file="config.yaml")["kowalski"]


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
