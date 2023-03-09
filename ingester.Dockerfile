FROM python:3.10

ARG scala_version=2.13
ARG kafka_version=3.4.0
ARG braai_version=d6_m9
ARG acai_h_version=d1_dnn_20201130
ARG acai_v_version=d1_dnn_20201130
ARG acai_o_version=d1_dnn_20201130
ARG acai_n_version=d1_dnn_20201130
ARG acai_b_version=d1_dnn_20201130

# Install vim, git, cron, and jdk
#RUN apt-get update && apt-get -y install apt-file && apt-file update && apt-get -y install vim && \
#    apt-get -y install git && apt-get install -y default-jdk

# place to keep our app and the data:
RUN mkdir -p /kowalski /kowalski/data /kowalski/logs /_tmp /kowalski/models/pgir /kowalski/models/ztf /kowalski/models/wntr /kowalski/models/turbo

WORKDIR /kowalski

# Install jdk, mkdirs, fetch and install Kafka
RUN apt-get update && apt-get install -y default-jdk && \
    wget https://downloads.apache.org/kafka/$kafka_version/kafka_$scala_version-$kafka_version.tgz -O kafka_$scala_version-$kafka_version.tgz && \
    tar -xzf kafka_$scala_version-$kafka_version.tgz

# Kafka test-server properties:
COPY server.properties kafka_$scala_version-$kafka_version/config/

# ZTF ML models <model_name>.<tag>.<extensions>:
ADD https://github.com/dmitryduev/braai/raw/master/models/braai_$braai_version.h5 models/ztf/braai.$braai_version.h5
ADD https://github.com/dmitryduev/acai/raw/master/models/acai_h.$acai_h_version.h5 models/ztf/
ADD https://github.com/dmitryduev/acai/raw/master/models/acai_v.$acai_v_version.h5 models/ztf/
ADD https://github.com/dmitryduev/acai/raw/master/models/acai_o.$acai_o_version.h5 models/ztf/
ADD https://github.com/dmitryduev/acai/raw/master/models/acai_n.$acai_n_version.h5 models/ztf/
ADD https://github.com/dmitryduev/acai/raw/master/models/acai_b.$acai_b_version.h5 models/ztf/

# copy over the test data
COPY data/ztf_alerts/ data/ztf_alerts/
COPY data/pgir_alerts/ data/pgir_alerts/
COPY data/wntr_alerts/ data/wntr_alerts/
COPY data/catalogs/ data/catalogs/
COPY data/ztf_matchfiles/ data/ztf_matchfiles/
COPY data/ztf_source_features/ data/ztf_source_features/
COPY data/ztf_source_classifications/ data/ztf_source_classifications/
COPY data/turbo_alerts/ data/turbo_alerts/

COPY requirements_ingester.txt .

COPY config.yaml .
COPY version.txt .

COPY kowalski/__init__.py kowalski/
COPY kowalski/utils.py kowalski/

COPY kowalski/tools/__init__.py kowalski/tools/
COPY kowalski/tools/check_db_entries.py kowalski/tools/
COPY kowalski/tools/generate_supervisord_conf.py kowalski/tools/
COPY kowalski/tools/ops_watcher_ztf.py kowalski/tools/
COPY kowalski/tools/tns_watcher.py kowalski/tools/
COPY kowalski/tools/performance_reporter.py kowalski/tools/
COPY kowalski/tools/fetch_ztf_matchfiles.py kowalski/tools/
COPY kowalski/tools/istarmap.py kowalski/tools/

COPY kowalski/dask_clusters/__init__.py kowalski/dask_clusters/
COPY kowalski/dask_clusters/dask_cluster.py kowalski/dask_clusters/
COPY kowalski/dask_clusters/dask_cluster_pgir.py kowalski/dask_clusters/
COPY kowalski/dask_clusters/dask_cluster_winter.py kowalski/dask_clusters/
COPY kowalski/dask_clusters/dask_cluster_turbo.py kowalski/dask_clusters/

COPY kowalski/alert_brokers/__init__.py kowalski/alert_brokers/
COPY kowalski/alert_brokers/alert_broker.py kowalski/alert_brokers/
COPY kowalski/alert_brokers/alert_broker_ztf.py kowalski/alert_brokers/
COPY kowalski/alert_brokers/alert_broker_pgir.py kowalski/alert_brokers/
COPY kowalski/alert_brokers/alert_broker_winter.py kowalski/alert_brokers/
COPY kowalski/alert_brokers/alert_broker_turbo.py kowalski/alert_brokers/

COPY kowalski/ingesters/__init__.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_catalog.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_gaia_edr3.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_igaps.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_ps1_strm.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_ptf_matchfiles.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_turbo.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_vlass.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_ztf_matchfiles.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_ztf_public.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_ztf_source_classifications.py kowalski/ingesters/
COPY kowalski/ingesters/ingest_ztf_source_features.py kowalski/ingesters/
COPY kowalski/ingesters/ingester.py kowalski/ingesters/

COPY kowalski/tests/test_alert_broker_ztf.py kowalski/tests/
COPY kowalski/tests/test_alert_broker_pgir.py kowalski/tests/
COPY kowalski/tests/test_alert_broker_wntr.py kowalski/tests/
COPY kowalski/tests/test_alert_broker_turbo.py kowalski/tests/
COPY kowalski/tests/test_ingester_ztf.py kowalski/tests/
COPY kowalski/tests/test_ingester_pgir.py kowalski/tests/
COPY kowalski/tests/test_ingester_wntr.py kowalski/tests/
COPY kowalski/tests/test_ingester_turbo.py kowalski/tests/
COPY kowalski/tests/test_tns_watcher.py kowalski/tests/
COPY kowalski/tests/test_tools.py kowalski/tests/

ENV USING_DOCKER=true

# update pip
RUN pip install --upgrade pip

# install python libs and generate supervisord config file
RUN pip install -r requirements_ingester.txt --no-cache-dir && \
    python kowalski/tools/generate_supervisord_conf.py ingester

# run container
CMD /usr/local/bin/supervisord -n -c supervisord_ingester.conf
