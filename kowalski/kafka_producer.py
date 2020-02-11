import argparse
import datetime
import os
import subprocess

from utils import load_config


''' load config and secrets '''
config = load_config(config_file='config_ingester.json')


def main():

    # start ZooKeeper in the background (using Popen and not run with shell=True for safety)
    cmd_zookeeper = [os.path.join(config['path']['kafka'], 'bin', 'zookeeper-server-start.sh'),
                     os.path.join(config['path']['kafka'], 'config', 'config/zookeeper.properties')]

    # subprocess.Popen(cmd_zookeeper, stdout=subprocess.PIPE)
    p_zookeeper = subprocess.Popen(cmd_zookeeper, stdout=subprocess.PIPE)

    # start the Kafka server:
    cmd_kafka_server = [os.path.join(config['path']['kafka'], 'bin', 'kafka-server-start.sh'),
                        os.path.join(config['path']['kafka'], 'config', 'config/server.properties')]

    # subprocess.Popen(cmd_kafka_server, stdout=subprocess.PIPE)
    p_kafka_server = subprocess.Popen(cmd_kafka_server, stdout=subprocess.PIPE)

    # create a test ZTF topic for the current UTC date
    topic_name = f'ztf_{datetime.datetime.utcnow().strftime("%Y%m%d")}_programid1_test'
    cmd_create_topic = [os.path.join(config['path']['kafka'], 'bin', 'kafka-topics.sh'),
                        "--create", "--bootstrap-server", "localhost:9092", "--replication-factor", "1",
                        "--partitions", "1", "--topic", topic_name]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Spin up a Kafka producer for testing')
    # parser.add_argument('--date', help='current date to name a topic')

    args = parser.parse_args()

    main()
