import datetime
import os

LOG_DIR = "./logs"
USING_DOCKER = os.environ.get("USING_DOCKER", False)
if USING_DOCKER:
    LOG_DIR = "/data/logs"


def time_stamp():
    """

    :return: UTC time -> string
    """
    return datetime.datetime.utcnow().strftime("%Y%m%d_%H:%M:%S")


def log(message):
    timestamp = time_stamp()
    print(f"{timestamp}: {message}")

    if not os.path.isdir(LOG_DIR):
        os.makedirs(LOG_DIR, exist_ok=True)

    date = timestamp.split("_")[0]
    with open(os.path.join(LOG_DIR, f"kowalski_{date}.log"), "a") as logfile:
        logfile.write(f"{timestamp}: {message}\n")
        logfile.flush()
