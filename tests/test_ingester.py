from confluent_kafka import Producer
import datetime
import os
import pathlib
import requests
import subprocess
import time

from alert_broker_ztf import watchdog
from utils import init_db_sync, load_config, log, Mongo


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


class Program:
    def __init__(self, group_name="FRITZ_TEST", group_nickname="Fritz"):
        self.access_token = config["skyportal"]["token"]
        self.base_url = (
            f"{config['skyportal']['protocol']}://"
            f"{config['skyportal']['host']}:{config['skyportal']['port']}"
        )
        self.headers = {"Authorization": f"token {self.access_token}"}

        self.group_name = group_name
        self.group_nickname = group_nickname
        self.group_id, self.filter_id = self.create()

    def get_groups(self):
        resp = requests.get(
            self.base_url + "/api/groups",
            headers=self.headers,
            timeout=3,
        )

        assert resp.status_code == requests.codes.ok
        result = resp.json()
        # print(result)
        assert result["status"] == "success"
        assert "data" in result
        assert "user_groups" in result["data"]

        user_groups = {
            g["name"]: g["id"] for g in result["data"]["user_accessible_groups"]
        }

        return user_groups

    def get_group_filters(self, group_id: int):
        resp = requests.get(
            self.base_url + "/api/filters",
            headers=self.headers,
            timeout=3,
        )

        assert resp.status_code == requests.codes.ok
        result = resp.json()
        # print(result)
        assert result["status"] == "success"
        assert "data" in result

        group_filter_ids = [
            fi["id"] for fi in result["data"] if fi["group_id"] == group_id
        ]

        return group_filter_ids

    def create(self):
        user_groups = self.get_groups()

        stream_ids = [1, 2]

        if self.group_name in user_groups.keys():
            # already exists? grab its id then:
            group_id = user_groups[self.group_name]
        else:
            # else, create a new group and add stream access to it
            resp = requests.post(
                self.base_url + "/api/groups",
                json={"name": self.group_name, "nickname": self.group_nickname},
                headers=self.headers,
                timeout=3,
            )
            result = resp.json()
            # print(result)
            assert result["status"] == "success"
            assert "data" in result
            assert "id" in result["data"]

            group_id = result["data"]["id"]

            # grant stream access to group
            for stream_id in stream_ids:
                resp = requests.post(
                    self.base_url + f"/api/groups/{group_id}/streams",
                    json={"stream_id": stream_id},
                    headers=self.headers,
                    timeout=3,
                )
                result = resp.json()
                # print(result)
                assert result["status"] == "success"
                assert result["data"]["stream_id"] == stream_id

        # grab filter_ids defined for this group:
        group_filter_ids = self.get_group_filters(group_id=group_id)

        if len(group_filter_ids) == 0:
            # none created so far? make one:
            resp = requests.post(
                self.base_url + "/api/filters",
                json={
                    "name": "Orange Transients",
                    "stream_id": max(stream_ids),
                    "group_id": group_id,
                },
                headers=self.headers,
                timeout=3,
            )
            result = resp.json()
            # print(result)
            assert result["status"] == "success"
            assert "data" in result
            assert "id" in result["data"]

            filter_id = result["data"]["id"]
        else:
            # else just grab the first one
            filter_id = group_filter_ids[0]

        return group_id, filter_id

    def remove(self):
        user_groups = self.get_groups()

        group_filter_ids = self.get_group_filters(group_id=self.group_id)
        for filter_id in group_filter_ids:
            resp = requests.delete(
                self.base_url + f"/api/filters/{filter_id}",
                headers=self.headers,
                timeout=3,
            )
            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"

        if self.group_name in user_groups.keys():

            resp = requests.delete(
                self.base_url + f"/api/groups/{user_groups[self.group_name]}",
                headers=self.headers,
                timeout=3,
            )
            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"

        self.group_id, self.filter_id = None, None


class Filter:
    def __init__(
        self,
        collection: str = "ZTF_alerts",
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
        self.collection = collection
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
                        "annotations.mean_rb": {"$avg": "$prv_candidates.rb"},
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
        # print(result)
        assert result["status"] == "success"
        assert "data" in result
        assert "fid" in result["data"]
        fid = result["data"]["fid"]

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
    End-to-end ingester test:
        - Create test program in Fritz
        - Spin up ZooKeeper
        - Spin up Kafka server
        - Create topic
        - Publish test alerts to topic
        - Create test filter
        - Spin up ingester
        - Digest and ingest alert stream, post to Fritz
        - Delete test filter
    """

    def test_ingester(self):

        init_db_sync(config=config, verbose=True)

        log("Setting up paths")
        # path_kafka = pathlib.Path(config["path"]["kafka"])

        path_logs = pathlib.Path(config["path"]["logs"])
        if not path_logs.exists():
            path_logs.mkdir(parents=True, exist_ok=True)

        if config["misc"]["broker"]:
            log("Setting up test groups and filters in Fritz")
            program = Program(group_name="FRITZ_TEST", group_nickname="test")
            Filter(
                collection="ZTF_alerts",
                group_id=program.group_id,
                filter_id=program.filter_id,
            )

            program2 = Program(group_name="FRITZ_TEST_AUTOSAVE", group_nickname="test2")
            Filter(
                collection="ZTF_alerts",
                group_id=program2.group_id,
                filter_id=program2.filter_id,
                autosave=True,
                pipeline=[{"$match": {"objectId": "ZTF20aaelulu"}}],
            )

            program3 = Program(
                group_name="FRITZ_TEST_UPDATE_ANNOTATIONS", group_nickname="test3"
            )
            Filter(
                collection="ZTF_alerts",
                group_id=program3.group_id,
                filter_id=program3.filter_id,
                update_annotations=True,
                pipeline=[
                    {"$match": {"objectId": "ZTF20aapcmur"}}
                ],  # there are 3 alerts in the test set for this oid
            )

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

        log("Starting up Kafka Server at localhost:9092")

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

        # get kafka topic names with kafka-topics command
        cmd_topics = [
            os.path.join(config["path"]["kafka"], "bin", "kafka-topics.sh"),
            "--zookeeper",
            config["kafka"]["zookeeper.test"],
            "-list",
        ]

        topics = (
            subprocess.run(cmd_topics, stdout=subprocess.PIPE)
            .stdout.decode("utf-8")
            .split("\n")[:-1]
        )
        log(f"Found topics: {topics}")

        # create a test ZTF topic for the current UTC date
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f"ztf_{date}_programid1_test"

        if topic_name in topics:
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
            log(f"{remove_topic}")
            log(f"Removed topic: {topic_name}")
            time.sleep(1)

        if topic_name not in topics:
            log(f"Creating topic {topic_name}")

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
        path_alerts = pathlib.Path("/app/data/ztf_alerts/20200202/")
        # grab some more alerts from gs://ztf-fritz/sample-public-alerts
        try:
            log("Grabbing more alerts from gs://ztf-fritz/sample-public-alerts")
            r = requests.get("https://www.googleapis.com/storage/v1/b/ztf-fritz/o")
            aa = r.json()["items"]
            ids = [pathlib.Path(a["id"]).parent for a in aa if "avro" in a["id"]]
        except Exception as e:
            log(
                "Grabbing alerts from gs://ztf-fritz/sample-public-alerts failed, but it is ok"
            )
            log(f"{e}")
            ids = []
        subprocess.run(
            [
                "gsutil",
                "-m",
                "cp",
                "-n",
                "gs://ztf-fritz/sample-public-alerts/*.avro",
                "/app/data/ztf_alerts/20200202/",
            ]
        )
        log(f"Fetched {len(ids)} alerts from gs://ztf-fritz/sample-public-alerts")
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

        log("Starting up Ingester")

        # digest and ingest
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

        log("Checking the ZTF alert collection states")
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=True,
        )
        collection_alerts = config["database"]["collections"]["alerts_ztf"]
        collection_alerts_aux = config["database"]["collections"]["alerts_ztf_aux"]

        num_retries = 10
        # alert processing takes time, which depends on the available resources
        # so allow some additional time for the processing to finish
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError("Alert ingestion failed")

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            try:
                assert n_alerts == 313
                assert n_alerts_aux == 145
                break
            except AssertionError:
                print(
                    "Found an unexpected amount of alert/aux data: "
                    f"({n_alerts}/{n_alerts_aux}, expecting 313/145). "
                    "Retrying in 30 seconds..."
                )
                time.sleep(30)
                continue

        if config["misc"]["broker"]:
            log("Checking that posting to SkyPortal succeeded")

            # check number of candidates that passed the first filter
            resp = requests.get(
                program.base_url + f"/api/candidates?groupIDs={program.group_id}",
                headers=program.headers,
                timeout=3,
            )

            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"
            assert "data" in result
            assert "totalMatches" in result["data"]
            assert result["data"]["totalMatches"] == 88

            # check that the only candidate that passed the second filter (ZTF20aaelulu) got saved as Source
            resp = requests.get(
                program2.base_url + f"/api/sources?group_ids={program2.group_id}",
                headers=program2.headers,
                timeout=3,
            )

            assert resp.status_code == requests.codes.ok
            result = resp.json()
            assert result["status"] == "success"
            assert "data" in result
            assert "totalMatches" in result["data"]
            assert result["data"]["totalMatches"] == 1
            assert "sources" in result["data"]
            assert result["data"]["sources"][0]["id"] == "ZTF20aaelulu"
