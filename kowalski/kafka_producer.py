import argparse
from confluent_kafka import Producer
import datetime
import os
import pathlib
import subprocess
import time

from utils import load_config


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main():

    if not os.path.exists(config["path"]["logs"]):
        os.makedirs(config["path"]["logs"])

    # start ZooKeeper in the background (using Popen and not run with shell=True for safety)
    cmd_zookeeper = [
        os.path.join(config["path"]["kafka"], "bin", "zookeeper-server-start.sh"),
        os.path.join(config["path"]["kafka"], "config", "zookeeper.properties"),
    ]

    # subprocess.Popen(cmd_zookeeper, stdout=subprocess.PIPE)
    with open(
        os.path.join(config["path"]["logs"], "zookeeper.stdout"), "w"
    ) as stdout_zookeeper:
        # p_zookeeper = \
        subprocess.Popen(
            cmd_zookeeper, stdout=stdout_zookeeper, stderr=subprocess.STDOUT
        )

    # take a nap while it fires up
    time.sleep(3)

    # start the Kafka server:
    cmd_kafka_server = [
        os.path.join(config["path"]["kafka"], "bin", "kafka-server-start.sh"),
        os.path.join(config["path"]["kafka"], "config", "server.properties"),
    ]

    # subprocess.Popen(cmd_kafka_server, stdout=subprocess.PIPE)
    # p_kafka_server = subprocess.Popen(cmd_kafka_server, stdout=subprocess.PIPE)
    with open(
        os.path.join(config["path"]["logs"], "kafka_server.stdout"), "w"
    ) as stdout_kafka_server:
        # p_kafka_server = \
        subprocess.Popen(
            cmd_kafka_server, stdout=stdout_kafka_server, stderr=subprocess.STDOUT
        )

    # take a nap while it fires up
    time.sleep(3)

    # get kafka topic names with kafka-topics command
    cmd_topics = [
        os.path.join(config["path"]["kafka"], "bin", "kafka-topics.sh"),
        "--zookeeper",
        config["kafka"]["zookeeper.test"],
        "-list",
    ]
    # print(kafka_cmd)

    topics = (
        subprocess.run(cmd_topics, stdout=subprocess.PIPE)
        .stdout.decode("utf-8")
        .split("\n")[:-1]
    )
    print(topics)

    # create a test ZTF topic for the current UTC date
    topic_name = f'ztf_{datetime.datetime.utcnow().strftime("%Y%m%d")}_programid1_test'
    if topic_name not in topics:
        cmd_create_topic = [
            os.path.join(config["path"]["kafka"], "bin", "kafka-topics.sh"),
            "--create",
            "--bootstrap-server",
            config["kafka"]["bootstrap.test.servers"],
            "--replication-factor",
            "1",
            "--partitions",
            "1",
            "--topic",
            topic_name,
        ]
        with open(
            os.path.join(config["path"]["logs"], "create_topic.stdout"), "w"
        ) as stdout_create_topic:
            # p_create_topic = \
            subprocess.run(
                cmd_create_topic, stdout=stdout_create_topic, stderr=subprocess.STDOUT
            )

    # spin up Kafka producer
    producer = Producer(
        {"bootstrap.servers": config["kafka"]["bootstrap.test.servers"]}
    )

    path_alerts = pathlib.Path("/data/ztf_alerts/20200202/")
    for p in path_alerts.glob("*.avro"):
        with open(str(p), "rb") as data:
            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)

            print(f"Pushing {p}")

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            producer.produce(topic_name, data.read(), callback=delivery_report)

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spin up a Kafka producer for testing")
    # parser.add_argument('--date', help='date to name a topic')

    args = parser.parse_args()

    main()
