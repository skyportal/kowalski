from confluent_kafka import Producer
import datetime
import os
import pathlib
import subprocess
import time

from alert_watcher_ztf import ingester
from utils import load_config, time_stamp


''' load config and secrets '''
config = load_config(config_file='config_ingester.json')


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'{time_stamp()}: Message delivery failed: {err}')
    else:
        print(f'{time_stamp()}: Message delivered to {msg.topic()} [{msg.partition()}]')


class TestIngester(object):
    """
        End-to-end ingester test:
            - Spin up ZooKeeper
            - Spin up Kafka server
            - create topic
            - publish test alerts to topic
            - spin up ingester
            - digest and ingest alert stream
    """

    def test_ingester(self):

        if not os.path.exists(config['path']['path_logs']):
            os.makedirs(config['path']['path_logs'])

        print(f'{time_stamp()}: Starting up ZooKeeper at localhost:2181')

        # start ZooKeeper in the background (using Popen and not run with shell=True for safety)
        cmd_zookeeper = [os.path.join(config['path']['path_kafka'], 'bin', 'zookeeper-server-start.sh'),
                         os.path.join(config['path']['path_kafka'], 'config', 'zookeeper.properties')]

        # subprocess.Popen(cmd_zookeeper, stdout=subprocess.PIPE)
        with open(os.path.join(config['path']['path_logs'], 'zookeeper.stdout'), 'w') as stdout_zookeeper:
            p_zookeeper = subprocess.Popen(cmd_zookeeper, stdout=stdout_zookeeper, stderr=subprocess.STDOUT)

        # take a nap while it fires up
        time.sleep(3)

        print(f'{time_stamp()}: Starting up Kafka Server at localhost:9092')

        # start the Kafka server:
        cmd_kafka_server = [os.path.join(config['path']['path_kafka'], 'bin', 'kafka-server-start.sh'),
                            os.path.join(config['path']['path_kafka'], 'config', 'server.properties')]

        # subprocess.Popen(cmd_kafka_server, stdout=subprocess.PIPE)
        # p_kafka_server = subprocess.Popen(cmd_kafka_server, stdout=subprocess.PIPE)
        with open(os.path.join(config['path']['path_logs'], 'kafka_server.stdout'), 'w') as stdout_kafka_server:
            p_kafka_server = subprocess.Popen(cmd_kafka_server, stdout=stdout_kafka_server, stderr=subprocess.STDOUT)

        # take a nap while it fires up
        time.sleep(3)

        # get kafka topic names with kafka-topics command
        cmd_topics = [os.path.join(config['path']['path_kafka'], 'bin', 'kafka-topics.sh'),
                      '--zookeeper', config['kafka']['zookeeper.test'],
                      '-list']

        topics = subprocess.run(cmd_topics, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')[:-1]
        print(f'{time_stamp()}: Found topics: {topics}')

        # create a test ZTF topic for the current UTC date
        date = datetime.datetime.utcnow().strftime("%Y%m%d")
        topic_name = f'ztf_{date}_programid1_test'

        if topic_name in topics:
            # topic previously created? remove first
            cmd_remove_topic = [os.path.join(config['path']['path_kafka'], 'bin', 'kafka-topics.sh'),
                                '--zookeeper', config['kafka']['zookeeper.test'],
                                '--delete', '--topic', topic_name]
            # print(kafka_cmd)
            remove_topic = subprocess.run(cmd_remove_topic,
                                          stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')[:-1]
            print(f'{time_stamp()}: {remove_topic}')
            print(f'{time_stamp()}: Removed topic: {topic_name}')
            time.sleep(1)

        if topic_name not in topics:
            print(f'{time_stamp()}: Creating topic {topic_name}')

            cmd_create_topic = [os.path.join(config['path']['path_kafka'], 'bin', 'kafka-topics.sh'),
                                "--create",
                                "--bootstrap-server", config['kafka']['bootstrap.test.servers'],
                                "--replication-factor", "1",
                                "--partitions", "1",
                                "--topic", topic_name]
            with open(os.path.join(config['path']['path_logs'], 'create_topic.stdout'), 'w') as stdout_create_topic:
                p_create_topic = subprocess.run(cmd_create_topic, stdout=stdout_create_topic, stderr=subprocess.STDOUT)

        print(f'{time_stamp()}: Starting up Kafka Producer')

        # spin up Kafka producer
        producer = Producer({'bootstrap.servers': config['kafka']['bootstrap.test.servers']})

        path_alerts = pathlib.Path('/app/data/ztf_alerts/20200202/')
        for p in path_alerts.glob('*.avro'):
            with open(str(p), 'rb') as data:
                # Trigger any available delivery report callbacks from previous produce() calls
                producer.poll(0)

                print(f'{time_stamp()}: Pushing {p}')

                # Asynchronously produce a message, the delivery report callback
                # will be triggered from poll() above, or flush() below, when the message has
                # been successfully delivered or failed permanently.
                producer.produce(topic_name, data.read(), callback=delivery_report)

                # Wait for any outstanding messages to be delivered and delivery report
                # callbacks to be triggered.
        producer.flush()

        print(f'{time_stamp()}: Starting up Ingester')

        # digest and ingest
        ingester(obs_date=date, save_packets=False, test=True)
        print(f'{time_stamp()}: Digested and ingested: all done!')

        print(f'{time_stamp()}: Shutting down Kafka Server at localhost:9092')
        # start the Kafka server:
        cmd_kafka_server_stop = [os.path.join(config['path']['path_kafka'], 'bin', 'kafka-server-stop.sh'),
                                 os.path.join(config['path']['path_kafka'], 'config', 'server.properties')]

        with open(os.path.join(config['path']['path_logs'], 'kafka_server.stdout'), 'w') as stdout_kafka_server:
            p_kafka_server_stop = subprocess.run(cmd_kafka_server_stop,
                                                 stdout=stdout_kafka_server, stderr=subprocess.STDOUT)

        print(f'{time_stamp()}: Shutting down ZooKeeper at localhost:2181')

        # start ZooKeeper in the background (using Popen and not run with shell=True for safety)
        cmd_zookeeper_stop = [os.path.join(config['path']['path_kafka'], 'bin', 'zookeeper-server-stop.sh'),
                              os.path.join(config['path']['path_kafka'], 'config', 'zookeeper.properties')]

        with open(os.path.join(config['path']['path_logs'], 'zookeeper.stdout'), 'w') as stdout_zookeeper:
            p_zookeeper_stop = subprocess.run(cmd_zookeeper_stop, stdout=stdout_zookeeper, stderr=subprocess.STDOUT)

        # shut down Kafka server and ZooKeeper?
        time.sleep(15)
        p_zookeeper.kill()
        p_kafka_server.kill()
