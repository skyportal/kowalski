from confluent_kafka import Producer
import datetime
import os
import pathlib
import requests
import subprocess
import time

from alert_broker_winter import watchdog
from test_ingester import Program
from utils import init_db_sync, load_config, log, Mongo


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]

class Filter:
    def __init__(
        self,
        collection: str = "WNTR_alerts",
        group_id=None,
        filter_id=None,
        permissions=None,
        autosave: bool = False,
        update_annotations: bool = False,
        pipeline=None,
    ):
        assert group_id is not None
        assert filter_id is not None

        self.access_token = self.get_api_token()
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        self.group_id = int(group_id)
        self.filter_id = int(filter_id)
        self.autosave = autosave
        self.update_annotations = update_annotations
        self.permissions = permissions if permissions is not None else [1, 2]

        self.pipeline = pipeline
        if self.pipeline is None:
            self.pipeline = [
                {
                    "$match": {
                        "candidate.drb": {"$gt": 0.9},
                    }
                },
                {
                    "$addFields": {
                        "annotations.author": "dd",
                        "annotations.mean_rb": {"$avg": "$prv_candidates.drb"},
                    }
                },
                {"$project": {"_id": 0, "candid": 1, "objectId": 1, "annotations": 1}},
            ]


        self.fid = self.create()

    @staticmethod
    def get_api_token():
        a = requests.post(
            f"http://kowalski_api_1:{config['server']['port']}/api/auth",
            json={
                "username": config["server"]["admin_username"],
                "password": config["server"]["admin_password"],
            },
        )
        credentials = a.json()
        token = credentials.get("token", None)
        print(f'token: {token}')

        return token

    def create(self):

        user_filter = {
            "group_id": self.group_id,
            "filter_id": self.filter_id,
            "catalog": self.collection,
            "permissions": self.permissions,
            "autosave": self.autosave,
            "update_annotations": self.update_annotations,
            "pipeline": self.pipeline,
        }

        # save:
        resp = requests.post(
            f"http://kowalski_api_1:{config['server']['port']}/api/filters",
            json=user_filter,
            headers=self.headers,
            timeout=5,
        )
        assert resp.status_code == requests.codes.ok
        result = resp.json()
        print(f'result: {result}')
        assert result["status"] == "success"
        assert "data" in result
        assert "fid" in result["data"]
        fid = result["data"]["fid"]
        print(f'fid: {fid}')
        return fid     

    def remove(self):
        resp = requests.delete(
            f"http://kowalski_api_1:{config['server']['port']}/api/filters",
            json={"group_id": self.group_id, "filter_id": self.filter_id},
            headers=self.headers,
            timeout=5,
        )
        assert resp.status_code == requests.codes.ok
        result = resp.json()
        assert result["status"] == "success"
        assert (
            result["message"]
            == f"removed filter for group_id={self.group_id}, filter_id={self.filter_id}"
        )

        self.fid = None

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        log(f"Message delivery failed: {err}")
    else:
        log(f"Message delivered to {msg.topic()} [{msg.partition()}]")

class TestIngester:
    """
    
    """
    def test_ingester(self):
        init_db_sync(config=config, verbose=True)

        log("Setting up paths")
        # path_kafka = pathlib.Path(config["path"]["kafka"])

        path_logs = pathlib.Path(config["path"]["logs"])
        if not path_logs.exists():
            path_logs.mkdir(parents=True, exist_ok=True)

        # TODO for testing SkyPortal connection
        # if config["misc"]["broker"]:
            # TODO copy over PGIR Fritz related setup
        
        # clean up old Kafka logs
        log("Cleaning up Kafka logs")
        subprocess.run(["rm", "-rf", path_logs / "kafka-logs", "/tmp/zookeeper"])

        log("Starting up ZooKeeper at localhost:2181")

        # start ZooKeeper in the background
        cmd_zookeeper = [
            os.path.join(config["path"]["kafka"], "bin", "zookeeper-server-start.sh"),
            "-daemon",
            os.path.join(config["path"]["kafka"], "config", "zookeeper.properties"),
        ]

        with open(path_logs / "zookeeper.stdout", "w") as stdout_zookeeper:
            # p_zookeeper =
            subprocess.run(
                cmd_zookeeper, stdout=stdout_zookeeper, stderr=subprocess.STDOUT
            )

        # take a nap while it fires up
        time.sleep(3)

        log(f'Starting up Kafka server')
        # start the Kafka server:
        cmd_kafka_server = [
            os.path.join(config["path"]["kafka"], "bin", "kafka-server-start.sh"),
            "-daemon",
            os.path.join(config["path"]["kafka"], "config", "server.properties"),
        ]

        with open(
            os.path.join(config["path"]["logs"], "kafka_server.stdout"), "w"
        ) as stdout_kafka_server:
            # p_kafka_server = subprocess.Popen(cmd_kafka_server, stdout=stdout_kafka_server, stderr=subprocess.STDOUT)
            # p_kafka_server =
            subprocess.run(cmd_kafka_server)

        # take a nap while it fires up
        time.sleep(3)
        log(f'Kafka server up!')

        # get kafka topic names with kafka-topics command
        cmd_topics = [
            os.path.join(config["path"]["kafka"], "bin", "kafka-topics.sh"),
            "--zookeeper",
            config["kafka"]["zookeeper.test"],
            "-list",
        ]

        log(f'Finding topics...')
        topics = (
            subprocess.run(cmd_topics, stdout=subprocess.PIPE)
            .stdout.decode("utf-8")
            .split("\n")[:-1]
        )
        log(f"Found topics: {topics}")

        # create a test WNTR topic for the current UTC date
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f"wntr_{date}_test"

        if topic_name in topics:
            log(f'Topic previously created, removing...')
            # topic previously created? remove first
            cmd_remove_topic = [
                os.path.join(config["path"]["kafka"], "bin", "kafka-topics.sh"),
                "--zookeeper",
                config["kafka"]["zookeeper.test"],
                "--delete",
                "--topic",
                topic_name,
            ]
            # print(kafka_cmd)
            remove_topic = (
                subprocess.run(cmd_remove_topic, stdout=subprocess.PIPE)
                .stdout.decode("utf-8")
                .split("\n")[:-1]
            )
            log(f"Remove topic command: {remove_topic}")
            log(f"Removed topic: {topic_name}")
            time.sleep(1)

        if topic_name not in topics:
            log(f"Topic doesn't exist, creating topic {topic_name}")

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
                    cmd_create_topic,
                    stdout=stdout_create_topic,
                    stderr=subprocess.STDOUT,
                )
        
        log("Starting up Kafka Producer")

        # spin up Kafka producer
        producer = Producer(
            {"bootstrap.servers": config["kafka"]["bootstrap.test.servers"]}
        )

        # small number of alerts that come with kowalski
        path_alerts = pathlib.Path("/app/data/wntr_alerts/20220815/")
        # fixme: ONLY USING THE ARCHIVAL WNTR ALERTS FOR NOW

        # push!
        for p in path_alerts.glob("*.avro"):
            with open(str(p), "rb") as data:
                # Trigger any available delivery report callbacks from previous produce() calls
                producer.poll(0)

                log(f"Pushing {p}")

                # Asynchronously produce a message, the delivery report callback
                # will be triggered from poll() above, or flush() below, when the message has
                # been successfully delivered or failed permanently.
                producer.produce(topic_name, data.read(), callback=delivery_report)

                # Wait for any outstanding messages to be delivered and delivery report
                # callbacks to be triggered.
        producer.flush()
        
        # digest and ingest
        mongo, collection_alerts, collection_alerts_aux = self.digest_and_ingest(date)

        num_retries = 10
        # alert processing takes time, which depends on the available resources
        # so allow some additional time for the processing to finish
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError("WNTR Alert ingestion failed")

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            # REMOVE THIS
            print("WNTR: Testing n_alerts and n_alerts_aux", n_alerts, n_alerts_aux)
            try:
                assert n_alerts == 4
                assert n_alerts_aux == 0
                break
            except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts}/{n_alerts_aux}, expecting 4/0). "
                    "Retrying in 30 seconds..."
                )
                time.sleep(30)
                continue



    def digest_and_ingest(self, date):
        # digest and ingest
        log(f'Calling watchdog')
        watchdog(obs_date=date, test=True)
        log("Digested and ingested: all done!")

        # shut down Kafka server and ZooKeeper
        time.sleep(30)

        log("Shutting down Kafka Server at localhost:9092")
        # start the Kafka server:
        cmd_kafka_server_stop = [
            os.path.join(config["path"]["kafka"], "bin", "kafka-server-stop.sh"),
            os.path.join(config["path"]["kafka"], "config", "server.properties"),
        ]

        with open(
            os.path.join(config["path"]["logs"], "kafka_server.stdout"), "w"
        ) as stdout_kafka_server:
            # p_kafka_server_stop = \
            subprocess.run(
                cmd_kafka_server_stop,
                stdout=stdout_kafka_server,
                stderr=subprocess.STDOUT,
            )

        log("Shutting down ZooKeeper at localhost:2181")
        cmd_zookeeper_stop = [
            os.path.join(config["path"]["kafka"], "bin", "zookeeper-server-stop.sh"),
            os.path.join(config["path"]["kafka"], "config", "zookeeper.properties"),
        ]

        with open(
            os.path.join(config["path"]["logs"], "zookeeper.stdout"), "w"
        ) as stdout_zookeeper:
            # p_zookeeper_stop = \
            subprocess.run(
                cmd_zookeeper_stop, stdout=stdout_zookeeper, stderr=subprocess.STDOUT
            )

        log("Checking the WNTR alert collection states")
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=True,
        )
        collection_alerts = config["database"]["collections"]["alerts_wntr"]
        collection_alerts_aux = config["database"]["collections"]["alerts_wntr_aux"]

        return mongo, collection_alerts, collection_alerts_aux


    def read_mongoDB(self):
        """Query mongoDB for number of rows/alerts in table."""
        print("Reading MongoDB collection states")
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=True,
        )
        collection_alerts_wntr = config["database"]["collections"]["alerts_wntr"]
        collection_alerts_aux_wntr = config["database"]["collections"]["alerts_wntr_aux"]

        n_alerts_wntr = mongo.db[collection_alerts_wntr].count_documents({})
        n_alerts_aux_wntr = mongo.db[collection_alerts_aux_wntr].count_documents({})

        collection_alerts_pgir = config["database"]["collections"]["alerts_pgir"]
        collection_alerts_aux_pgir = config["database"]["collections"]["alerts_pgir_aux"]

        n_alerts_pgir = mongo.db[collection_alerts_pgir].count_documents({})
        n_alerts_aux_pgir = mongo.db[collection_alerts_aux_pgir].count_documents({})

        # REMOVE THIS
        print(f"WINTER: {n_alerts_wntr} alerts, {n_alerts_aux_wntr} alerts_aux")
        print(f"PGIR: {n_alerts_pgir} alerts, {n_alerts_aux_pgir} alerts_aux")

        # TODO check
        try: 
            assert n_alerts_wntr == 4
            assert n_alerts_aux_wntr == 0
        except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts_wntr}/{n_alerts_aux_wntr}, expecting 4/0). "
                )


if __name__ == "__main__":
    testIngest = TestIngester()
    testIngest.test_ingester()

    # testFilter = Filter()
    # print(f'finished tests in __main__')