FROM python:3.7

ARG kafka_version=2.13-2.5.0
ARG braai_version=d6_m9
ARG acai_h_version=d1_dnn_20201130
ARG acai_v_version=d1_dnn_20201130
ARG acai_o_version=d1_dnn_20201130
ARG acai_n_version=d1_dnn_20201130
ARG acai_b_version=d1_dnn_20201130

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

# ML models <model_name>.<tag>.<extensions>:
ADD https://github.com/dmitryduev/braai/raw/master/models/braai_$braai_version.h5 /app/models/braai.$braai_version.h5
ADD https://github.com/dmitryduev/acai-pub/raw/master/models/acai_h.$acai_h_version.h5 /app/models/
ADD https://github.com/dmitryduev/acai-pub/raw/master/models/acai_v.$acai_v_version.h5 /app/models/
ADD https://github.com/dmitryduev/acai-pub/raw/master/models/acai_o.$acai_o_version.h5 /app/models/
ADD https://github.com/dmitryduev/acai-pub/raw/master/models/acai_n.$acai_n_version.h5 /app/models/
ADD https://github.com/dmitryduev/acai-pub/raw/master/models/acai_b.$acai_b_version.h5 /app/models/

# copy over the test data
COPY data/ztf_alerts/ /app/data/ztf_alerts/
COPY data/catalogs/ /app/data/catalogs/
COPY data/ztf_matchfiles/ /app/data/ztf_matchfiles/
COPY data/ztf_source_features/ /app/data/ztf_source_features/

# copy over the config and the code
COPY ["config.yaml", "version.txt", "kowalski/generate_supervisord_conf.py", "kowalski/utils.py",\
      "kowalski/dask_cluster.py",\
      "kowalski/alert_broker_ztf.py",\
      "kowalski/ops_watcher_ztf.py",\
      "kowalski/tns_watcher.py",\
      "kowalski/performance_reporter.py",\
      "kowalski/requirements_ingester.txt",\
      "tests/test_ingester.py", "tests/test_tns_watcher.py", "tests/test_tools.py",\
      "tools/fetch_ztf_matchfiles.py",\
      "tools/ingest_ztf_matchfiles.py", "tools/ingest_ztf_source_features.py",\
      "tools/istarmap.py",\
      "tools/ingest_vlass.py",\
      "tools/ingest_igaps.py",\
      "/app/"]

# change working directory to /app
WORKDIR /app

# install python libs and generate supervisord config file
RUN pip install -r /app/requirements_ingester.txt --no-cache-dir && \
    python generate_supervisord_conf.py ingester

# run container
CMD /usr/local/bin/supervisord -n -c supervisord_ingester.conf
