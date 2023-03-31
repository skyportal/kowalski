__all__ = ["GCNAlertConsumer", "GCNAlertWorker", "EopError"]

import datetime
import os
import traceback
import uuid
from typing import Mapping

import certifi
import confluent_kafka
import dask.distributed
import lxml
import xmlschema
from authlib.integrations.requests_client import OAuth2Session

from kowalski.config import load_config
from kowalski.log import log
from kowalski.tools.gcn_utils import (
    from_cone,
    from_url,
    get_contour,
    get_dateobs,
    get_skymap_metadata,
    get_trigger,
)
from kowalski.utils import Mongo, time_stamp, timer

# Tensorflow is problematic for Mac's currently, so we can add an option to disable it
USE_TENSORFLOW = os.environ.get("USE_TENSORFLOW", True) in [
    "True",
    "t",
    "true",
    "1",
    True,
    1,
]

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


def set_oauth_cb(config):
    """Implement client support for KIP-768 OpenID Connect.
    Apache Kafka 3.1.0 supports authentication using OpenID Client Credentials.
    Native support for Python is coming in the next release of librdkafka
    (version 1.9.0). Meanwhile, this is a pure Python implementation of the
    refresh token callback.
    """
    if config.pop("sasl.oauthbearer.method", None) != "oidc":
        return

    client_id = config.pop("sasl.oauthbearer.client.id")
    client_secret = config.pop("sasl.oauthbearer.client.secret", None)
    scope = config.pop("sasl.oauthbearer.scope", None)
    token_endpoint = config.pop("sasl.oauthbearer.token.endpoint.url")

    session = OAuth2Session(client_id, client_secret, scope=scope)

    def oauth_cb(*_, **__):
        token = session.fetch_token(token_endpoint, grant_type="client_credentials")
        return token["access_token"], token["expires_at"]

    config["oauth_cb"] = oauth_cb


class EopError(Exception):
    """
    Exception raised when reaching end of a Kafka topic partition.
    """

    def __init__(self, msg):
        """
        :param msg: The Kafka message result from consumer.poll()
        """
        message = (
            f"{time_stamp()}: topic:{msg.topic()}, partition:{msg.partition()}, "
            f"status:end, offset:{msg.offset()}, key:{str(msg.key())}\n"
        )
        self.message = message

    def __str__(self):
        return self.message


class GCNAlertConsumer:
    """
    Creates an alert stream Kafka consumer for a given topic.
    """

    def __init__(self, topics: list, dask_client: dask.distributed.Client, **kwargs):

        self.verbose = kwargs.get("verbose", 2)

        self.dask_client = dask_client

        self.topics = topics

        self.consumer = confluent_kafka.Consumer(**kwargs)
        self.num_partitions = 0

        def on_assign(consumer, partitions, _self=self):
            # force-reset offsets when subscribing to a topic:
            for part in partitions:
                # -2 stands for beginning and -1 for end
                part.offset = -2
                # keep number of partitions.
                # when reaching end of last partition, kill thread and start from beginning
                _self.num_partitions += 1
                log(consumer.get_watermark_offsets(part))

        self.consumer.subscribe(topics, on_assign=on_assign)
        log(f"Successfully subscribed to {len(topics)} topics: {topics}")

        # set up own mongo client
        self.collection_alerts = config["database"]["collections"]["alerts_gcn"]

        self.mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            srv=config["database"]["srv"],
            verbose=self.verbose,
        )

        # create indexes
        if config["database"]["build_indexes"]:
            for index in config["database"]["indexes"][self.collection_alerts]:
                try:
                    ind = [tuple(ii) for ii in index["fields"]]
                    self.mongo.db[self.collection_alerts].create_index(
                        keys=ind,
                        name=index["name"],
                        background=True,
                        unique=index["unique"],
                    )
                except Exception as e:
                    log(e)

        log("Finished AlertConsumer setup")

    @classmethod
    def decode_message(cls, msg):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """
        message = msg.value()
        return message

    @staticmethod
    def process_alert(alert: Mapping, topic: str):
        """Alert brokering task run by dask.distributed workers

        :param alert: decoded alert from Kafka stream
        :param topic: Kafka stream topic name for bookkeeping
        :return:
        """
        schema = "data/schema/VOEvent-v2.0.xsd"
        voevent_schema = xmlschema.XMLSchema(schema)
        if voevent_schema.is_valid(alert):
            # check if is string
            try:
                alert = alert.encode("ascii")
            except AttributeError:
                pass
            root = lxml.etree.fromstring(alert)
        else:
            raise ValueError("xml file is not valid VOEvent")

        dateobs = get_dateobs(root)
        triggerid = get_trigger(root)

        if dateobs is None and triggerid is None:
            log(f"Alert from {topic} has no dateobs or triggerid, skipping")
            return

        # get worker running current task
        worker = dask.distributed.get_worker()
        alert_worker = worker.plugins["worker-init"].alert_worker

        # we try to get by triggerid, if an alert already exists with that triggerid, we use its dateobs instead
        if triggerid is not None:
            existing = alert_worker.mongo.db[alert_worker.collection_alerts].find_one(
                {"triggerid": triggerid}, {"_id": 0, "dateobs": 1}
            )
            if existing is not None:
                dateobs = existing["dateobs"]

        alert = {
            "dateobs": dateobs,
            "triggerid": triggerid,
            "notice_type": topic,
            "notice_content": lxml.etree.tostring(root),
            "date_created": datetime.datetime.utcnow(),
        }

        # return if this alert packet has already been processed and ingested into collection_alerts:
        if (
            alert_worker.mongo.db[alert_worker.collection_alerts].count_documents(
                {"dateobs": dateobs, "triggerid": triggerid}, limit=1
            )
            == 1
        ):
            return

        # we generate the skymap
        with timer(
            f"Generating skymap for {dateobs} - {topic}", alert_worker.verbose > 1
        ):
            skymap = alert_worker.generate_skymap(root, topic)

        if skymap is not None:
            # we generate the contour
            with timer(
                f"Generating contour for {dateobs} - {topic}",
                alert_worker.verbose > 1,
            ):
                contours = alert_worker.generate_contours(skymap)
                alert["localization"] = contours

        with timer(f"Ingesting {dateobs} - {topic}", alert_worker.verbose > 1):
            alert_worker.mongo.insert_one(
                collection=alert_worker.collection_alerts, document=alert
            )

        del contours
        del skymap
        del alert
        del triggerid
        del dateobs
        del root

    def poll(self):
        """Polls Kafka broker to consume a topic."""
        msg = self.consumer.poll()

        if msg is None:
            log("Caught error: msg is None")

        if msg.error():
            # reached end of topic
            log(f"Caught error: {msg.error()}")
            pass

        elif msg is not None:
            try:

                msg_decoded = self.decode_message(msg)
                topic = msg.topic()

                with timer(
                    f"Submitting alert from topic {topic} for processing",
                    self.verbose > 1,
                ):
                    future = self.dask_client.submit(
                        self.process_alert, msg_decoded, topic, pure=True
                    )
                    dask.distributed.fire_and_forget(future)
                    future.release()
                    del future

            except Exception as e:
                print("Error in poll!")
                log(e)
                _err = traceback.format_exc()
                log(_err)


class GCNAlertWorker:
    """Tools to handle alert processing:
    database ingestion, filtering, ml'ing, cross-matches, reporting to SP"""

    def __init__(self, **kwargs):

        self.verbose = kwargs.get("verbose", 2)
        self.config = config

        # MongoDB collections to store the alerts:
        self.collection_alerts = self.config["database"]["collections"]["alerts_gcn"]
        self.collection_alerts_aux = self.config["database"]["collections"][
            "alerts_gcn_aux"
        ]

        self.mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            srv=config["database"]["srv"],
            verbose=self.verbose,
        )

    @staticmethod
    def generate_skymap(root, notice_type):
        """
        Generate a skymap from an alert

        :param alert:
        :return:
        """
        status, skymap_metadata = get_skymap_metadata(root, notice_type)

        if status == "available":
            return from_url(skymap_metadata["url"])
        elif status == "cone":
            return from_cone(
                ra=skymap_metadata["ra"],
                dec=skymap_metadata["dec"],
                error=skymap_metadata["error"],
            )
        else:
            return None

    @staticmethod
    def generate_contours(skymap):
        """
        Generate a contour from a skymap

        :param skymap:
        :return:
        """
        contours = get_contour(skymap)
        return {
            "center": contours[0],
            "contour50": contours[1],
            "contour90": contours[2],
        }


class WorkerInitializer(dask.distributed.WorkerPlugin):
    def __init__(self, *args, **kwargs):
        self.alert_worker = None

    def setup(self, worker: dask.distributed.Worker):
        self.alert_worker = GCNAlertWorker()


def topic_listener():
    """Listen to a Kafka topic with GCN alerts
    :return:
    """

    # first, we verify that there is a gcn key in the config
    if "gcn" not in config:
        raise ValueError("No GCN configuration found in config file")
    # if there is no client_id and client_secret, we show a warning and exit
    if "client_id" not in config["gcn"] or "client_secret" not in config["gcn"]:
        log(
            "No client_id or client_secret found in config file. Exiting the GCN alert broker."
        )
        return

    # if there is no group_id, we generate one
    if "client_group_id" not in config["gcn"]:
        config["gcn"]["client_group_id"] = f"{str(uuid.uuid4())}"
        log(
            f"No client_group_id found in config file. Using a randomly generated one: {config['gcn']['client_group_id']} (entire alert stream will be reprocessed everytime)"
        )

    # Configure dask client
    dask_client = dask.distributed.Client(
        address=f"{config['dask_gcn']['host']}:{config['dask_gcn']['scheduler_port']}"
    )

    # init each worker with AlertWorker instance
    worker_initializer = WorkerInitializer()
    dask_client.register_worker_plugin(worker_initializer, name="worker-init")
    # Configure consumer connection to Kafka broker
    domain = config["gcn"]["server"]
    topics = [
        f"gcn.classic.voevent.{notice_type}"
        for notice_type in config["gcn"]["notice_types"]
    ]

    conf = {
        "security.protocol": "sasl_ssl",
        "ssl.ca.location": certifi.where(),
        "bootstrap.servers": f"kafka.{domain}",
        "sasl.mechanisms": "OAUTHBEARER",
        "sasl.oauthbearer.method": "oidc",
        "sasl.oauthbearer.client.id": config["gcn"]["client_id"],
        "sasl.oauthbearer.client.secret": config["gcn"]["client_secret"],
        "sasl.oauthbearer.token.endpoint.url": f"https://auth.{domain}/oauth2/token",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "group.id": config["gcn"]["client_group_id"],
    }

    set_oauth_cb(conf)

    # Start alert stream consumer
    stream_reader = GCNAlertConsumer(topics, dask_client, **conf)

    while True:
        try:
            # poll!
            stream_reader.poll()
        except Exception as e:
            log(e)
            _err = traceback.format_exc()
            log(_err)
            break


if __name__ == "__main__":
    topic_listener()

# TODO: CROSSMATCH ALERTS WITH THAT COLLECTION, USING THAT QUERY:
# {
#   'contour50': {'$geoIntersects': {'$geometry': [-152, 68]}},
#   'dateobs': {
#       '$lte': ISODate('2023-03-25'),
#       '$gte': ISODate('2023-03-19')
#   }
# }
