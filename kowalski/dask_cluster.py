from dask.distributed import LocalCluster
import time

from utils import load_config, log


''' load config and secrets '''
# config = load_config(path='./', config_file='config.yaml')['kowalski']
config = load_config(config_file='config.yaml')['kowalski']


if __name__ == '__main__':

    cluster = LocalCluster(
        threads_per_worker=config['dask']['threads_per_worker'],
        n_workers=config['dask']['n_workers'],
        scheduler_port=config['dask']['scheduler_port'],
    )
    log(cluster)

    while True:
        time.sleep(60)
        log("Heartbeat")
