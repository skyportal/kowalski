import argparse
from ast import literal_eval
from copy import deepcopy
import datetime
from ensurepip import bootstrap
import io
import logging
import sys
import traceback
from typing import Mapping

# specific, copied from kowalski repo
# https://github.com/dmitryduev/kowalski
from winterdrp.utils_kowalski import (
    deg2dms,
    deg2hms,
    radec2lb,
    timer
)

import confluent_kafka
import fastavro

logger = logging.getLogger(__name__)  
logger.setLevel(logging.INFO)

class AlertConsumer:
    """
    Creates an alert stream Kafka consumer for a given topic,
    reads incoming packets, and ingests stream into database
    based on applied filters. 

    """
   
    def __init__(
        self,
        bootstrap_server_str:str, 
        topic:str,
        group_id:str,
        verbose = 2
        
    ):
        """
        Initializes Kafka consumer.

        :param bootstrap_server_str: IP addresses of brokers to subscribe to, comma-separated
        e.g. 192.168.0.64:9092,192.168.0.65:9092,192.168.0.66:9092
        :type bootstrap_server_str: str
        :param topic: name of topic to poll from, e.g. winter_20220728
        :type topic: str
        :param group_id: id, typically prefix of topic name, e.g. winter
        :type group_id: str
        :param verbose: _description_, defaults to 2
        :type verbose: int, optional
        """
        # Configure consumer connection to Kafka broker
        bootstrap_servers = {bootstrap_server_str} 
        logger.info(f'testingggggg')
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "default.topic.config": {"auto.offset.reset": "earliest"},
        }

        conf["group.id"] = group_id
        # make it unique
        conf["group.id"] = f"{conf['group.id']}_{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')}"
        
        # keep track of disconnected partitions
        self.num_disconnected_partitions = 0
        self.topic = topic
        self.verbose = verbose

        def error_cb(err, _self=self):
            print(f"error_cb --------> {err}")
            print(f'{err.code()}')
            if err.code() == -195:
                _self.num_disconnected_partitions += 1
                if _self.num_disconnected_partitions == _self.num_partitions:
                    print("All partitions got disconnected, killing thread")
                    sys.exit()
                else:
                    print(
                        f"{_self.topic}: disconnected from partition. total: {_self.num_disconnected_partitions}"
                    )

        self.consumer = confluent_kafka.Consumer(conf)
        self.num_partitions = 0

        def on_assign(consumer, partitions, _self=self):
            # force-reset offsets when subscribing to a topic:
            print(f'parititions: {partitions}')
            for part in partitions:
                # -2 stands for beginning and -1 for end
                part.offset = -2
                # keep number of partitions.
                # when reaching end of last partition, kill thread and start from beginning
                _self.num_partitions += 1
                print(f'Topic partition offsets: {consumer.get_watermark_offsets(part)}')

        self.consumer.subscribe([topic], on_assign=on_assign)
        print(f"Successfully subscribed to {topic}")

    @staticmethod
    def read_schema_data(bytes_io):
        """
        Read data that already has an Avro schema.

        :param bytes_io: `_io.BytesIO` Data to be decoded.
        :return: `dict` Decoded data.
        """
        bytes_io.seek(0)
        message = fastavro.reader(bytes_io)
        return message

    @classmethod
    def decode_message(cls, msg):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """
        message = msg.value()
        decoded_msg = message

        try:
            bytes_io = io.BytesIO(message)
            decoded_msg = cls.read_schema_data(bytes_io)
        except AssertionError:
            decoded_msg = None
        except IndexError:
            literal_msg = literal_eval(
                str(message, encoding="utf-8")
            )  # works to give bytes
            bytes_io = io.BytesIO(literal_msg)  # works to give <class '_io.BytesIO'>
            decoded_msg = cls.read_schema_data(bytes_io)  # yields reader
        except Exception:
            decoded_msg = message
        finally:
            return decoded_msg
        
    def alert_mongify(self, alert: Mapping):
        """
        Prepare a raw alert for ingestion into MongoDB:
          - add a placeholder for ML-based classifications
          - add coordinates for 2D spherical indexing and compute Galactic coordinates
          - cut off the prv_candidates section

        :param alert:
        :return:
        """
        doc = dict(alert)
        # placeholders for classifications
        doc["classifications"] = dict()

        # GeoJSON for 2D indexing
        doc["coordinates"] = {}
        _ra = doc["candidate"]["ra"]
        _dec = doc["candidate"]["dec"]
        # string format: H:M:S, D:M:S
        _radec_str = [deg2hms(_ra), deg2dms(_dec)]
        doc["coordinates"]["radec_str"] = _radec_str
        # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
        _radec_geojson = [_ra - 180.0, _dec]
        doc["coordinates"]["radec_geojson"] = {
            "type": "Point",
            "coordinates": _radec_geojson,
        }

        # Galactic coordinates l and b
        l, b = radec2lb(doc["candidate"]["ra"], doc["candidate"]["dec"])
        doc["coordinates"]["l"] = l
        doc["coordinates"]["b"] = b

        prv_candidates = deepcopy(doc["prv_candidates"])
        doc.pop("prv_candidates", None)
        if prv_candidates is None:
            prv_candidates = []

        return doc, prv_candidates

    
    def process_alert(self, alert: Mapping, topic: str):
        """
        Main function that runs on a single alert.
            -Read top-level packet field
            -Separate alert and prv_candidate in MongoDB prep

        :param alert: decoded alert from Kafka stream
        :param topic: Kafka stream topic name for bookkeeping
        :return:
        """
        candid = alert["candid"]
        object_id = alert["objectId"]
        candidate = alert["candidate"]

        print(f"{topic} {object_id} {candid} in process_alert")
        print(f"----------------------------------------------")
        print(f"{candidate}")
        print(f"----------------------------------------------")


        # candid not in db, ingest decoded avro packet into db
        with timer(f"Mongification of {object_id} {candid}"):
            alert, prv_candidates = self.alert_mongify(alert)

        # prv_candidates: pop nulls - save space
        prv_candidates = [
            {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
            for prv_candidate in prv_candidates
        ]

    def poll(self):
        """
        Polls Kafka broker to consume a topic.
        If receives message, for each packet:
            - decodes avro schema
            - processess alert (#TODO)
        
        """
        msg = self.consumer.poll()

        if msg is None:
            print("############################")
            print("Caught error: msg is None")
            print("############################")

        if msg.error():
            # reached end of topic
            print(f"Caught error: {msg.error()}")

        elif msg is not None:
            try:
                # decode avro packet
                with timer("Decoding alert", self.verbose > 1):
                    msg_decoded = self.decode_message(msg)
                    print(f'Avro packet decoded')

                for record in msg_decoded:
                        with timer(
                            f"Submitting alert {record['objectId']} {record['candid']} for processing",
                            self.verbose > 1,
                        ):
                            self.process_alert(record, self.topic)
                            print(f"Finished process_alert {record['objectId']} {record['candid']}")


            except Exception as e:
                print("Error in poll!")
                print(e)
                _err = traceback.format_exc()
                print(_err)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read broadcasts from a Kafka Producer")
    parser.add_argument("--topic", help="Topic name to query")
    parser.add_argument("--group_id", help="prefix of topic, bit redundant")
    parser.add_argument("--servers", help="comma-seperated string of server IP addresses")
    

    args = parser.parse_args()

    print(f'Starting up')
    test_consumer = AlertConsumer(args.servers, args.topic, args.group_id)
    print(f'Successfully created AlertConsumer')

    # For testing
    i = 1
    while i < 5:
        test_consumer.poll()
        print(f'Finished poll {i}')
        i += 1

    

