import argparse
import os
import pathlib

from kowalski.utils import load_config
from kowalski.ingesters.ingester import KafkaStream

KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/kowalski")
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
parser.add_argument("--action", type=str, help="start or stop the stream")

args = parser.parse_args()

topic = args.topic
path_alerts = args.path_alerts
test = args.test
action = args.action

if not isinstance(topic, str) or topic == "":
    raise ValueError("topic must be a non-empty string")

if not isinstance(path_alerts, str) or path_alerts == "":
    raise ValueError("path_alerts must be a non-empty string")

if not isinstance(test, bool):
    raise ValueError("test must be a boolean")

if isinstance(action, str) and action.lower() not in ["start", "stop"]:
    raise ValueError(
        "action must be either start or stop. Default is start if not specified"
    )
print("\nParameters:")
print(f"topic: {topic}")
print(f"path_alerts: {path_alerts}")
print(f"test: {test}")
print(f"action: {action}")

stream = KafkaStream(
    topic=topic,
    path_alerts=pathlib.Path(f"{KOWALSKI_APP_PATH}/data/{path_alerts}"),
    test=test,
    config=config,
)

if action == "stop":
    print("\nStopping Kafka stream...")
    stream.stop()
else:
    print("\nStarting Kafka stream...")
    stream.start()
