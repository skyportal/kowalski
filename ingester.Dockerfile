FROM python:3.7
#FROM python:3.7-slim

ARG kafka_version=2.13-2.5.0

# Install vim, git, cron, and jdk
#RUN apt-get update && apt-get -y install apt-file && apt-file update && apt-get -y install vim && \
#    apt-get -y install git && apt-get install -y default-jdk

# Install jdk, mkdirs, fetch and install Kafka
RUN apt-get update && apt-get install -y default-jdk && \
    mkdir -p /app /data /data/logs /_tmp /kafka && \
    wget https://storage.googleapis.com/ztf-fritz/kafka_$kafka_version.tgz -O /kafka/kafka_$kafka_version.tgz && \
    tar -xzf /kafka/kafka_$kafka_version.tgz

# Kafka:
#ADD http://apache.claz.org/kafka/2.5.0/kafka_$kafka_version.tgz /kafka
#RUN tar -xzf /kafka/kafka_$kafka_version.tgz

# Kafka test-server properties:
COPY kowalski/server.properties /kafka_$kafka_version/config/

# ML models:
ADD https://github.com/dmitryduev/kowalski/raw/master/kowalski/models/braai_d6_m9.h5 /app/models/

# copy over the test alerts
COPY data/ztf_alerts/ /app/data/ztf_alerts/

# copy over the config and the code
COPY ["config.yaml", "kowalski/generate_supervisord_conf.py", "kowalski/utils.py",\
      "kowalski/alert_watcher_ztf.py",\
      "kowalski/ops_watcher_ztf.py",\
      "kowalski/requirements_ingester.txt", "tests/test_ingester.py",\
      "/app/"]

# change working directory to /app
WORKDIR /app

# install python libs and generate supervisord config file
RUN pip install -r /app/requirements_ingester.txt --no-cache-dir && \
    python generate_supervisord_conf.py ingester

# run container
CMD /usr/local/bin/supervisord -n -c supervisord_ingester.conf
#CMD python api.py
