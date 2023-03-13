import argparse
import os
import pathlib
import time

from utils import load_config
from ingester import KafkaStream

KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/app")
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]

parser = argparse.ArgumentParser(description="Create a Kafka stream")

parser.add_argument("--topic", type=str, help="Kafka topic name")
parser.add_argument(
    "--path_alerts",
    type=str,
    help="path to the alerts directory in KOWALSKI_APP_PATH/data",
)
parser.add_argument(
    "--test",
    type=bool,
    help="test mode. if in test mode, alerts will be pushed to bootstarp.test.server",
)

args = parser.parse_args()

topic = args.topic
path_alerts = args.path_alerts
test = args.test

if not isinstance(topic, str) or topic == "":
    raise ValueError("topic must be a non-empty string")

if not isinstance(path_alerts, str) or path_alerts == "":
    raise ValueError("path_alerts must be a non-empty string")

if not isinstance(test, bool):
    raise ValueError("test must be a boolean")

print("\nParameters:")
print(f"topic: {topic}")
print(f"path_alerts: {path_alerts}")
print(f"test: {test}")


stream = KafkaStream(
    topic=topic,
    path_alerts=pathlib.Path(f"{KOWALSKI_APP_PATH}/data/{path_alerts}"),
    test=test,
    config=config,
)

running = True

while running:
    # if the user hits Ctrl+C, stop the stream
    try:
        stream.start()
        time.sleep(1000000000)
    except KeyboardInterrupt:
        print("\nStopping Kafka stream...")
        stream.stop()
        running = False
