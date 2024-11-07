FROM python:3.10

ARG scala_version=2.13
ARG kafka_version=3.4.1
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

# Install jdk, mkdirs, uv, fetch and install Kafka, create virtualenv
RUN apt-get update && apt-get install -y default-jdk && \
    wget https://archive.apache.org/dist/kafka/$kafka_version/kafka_$scala_version-$kafka_version.tgz --no-verbose -O kafka_$scala_version-$kafka_version.tgz && \
    tar -xzf kafka_$scala_version-$kafka_version.tgz && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    uv venv env --python=python3.10

# Kafka test-server properties:
COPY server.properties kafka_$scala_version-$kafka_version/config/

COPY requirements/ requirements/
COPY config.defaults.yaml config.defaults.yaml
COPY docker.yaml config.yaml
COPY version.txt .

# copy over the test data
COPY data/ztf_alerts/ data/ztf_alerts/
COPY data/pgir_alerts/ data/pgir_alerts/
COPY data/wntr_alerts/ data/wntr_alerts/
COPY data/catalogs/ data/catalogs/
COPY data/ztf_matchfiles/ data/ztf_matchfiles/
COPY data/ztf_source_features/ data/ztf_source_features/
COPY data/ztf_source_classifications/ data/ztf_source_classifications/
COPY data/turbo_alerts/ data/turbo_alerts/

COPY ["kowalski/__init__.py", \
    "kowalski/utils.py", \
    "kowalski/config.py", \
    "kowalski/log.py", \
    "kowalski/"]

# write the same copy lines above but as a single line:
COPY ["kowalski/tools/__init__.py", \
    "kowalski/tools/check_app_environment.py", \
    "kowalski/tools/check_db_entries.py", \
    "kowalski/tools/fetch_ztf_alerts.py", \
    "kowalski/tools/fetch_ztf_matchfiles.py", \
    "kowalski/tools/generate_supervisord_conf.py", \
    "kowalski/tools/init_models.py", \
    "kowalski/tools/init_kafka.py", \
    "kowalski/tools/istarmap.py", \
    "kowalski/tools/kafka_stream.py", \
    "kowalski/tools/ops_watcher_ztf.py", \
    "kowalski/tools/performance_reporter.py", \
    "kowalski/tools/install_python_requirements.py", \
    "kowalski/tools/tns_watcher.py", \
    "kowalski/tools/watch_logs.py", \
    "kowalski/tools/"]

COPY ["kowalski/dask_clusters/__init__.py", \
    "kowalski/dask_clusters/dask_cluster.py", \
    "kowalski/dask_clusters/dask_cluster_pgir.py", \
    "kowalski/dask_clusters/dask_cluster_winter.py", \
    "kowalski/dask_clusters/dask_cluster_turbo.py", \
    "kowalski/dask_clusters/"]

COPY ["kowalski/alert_brokers/__init__.py", \
    "kowalski/alert_brokers/alert_broker.py", \
    "kowalski/alert_brokers/alert_broker_ztf.py", \
    "kowalski/alert_brokers/alert_broker_pgir.py", \
    "kowalski/alert_brokers/alert_broker_winter.py", \
    "kowalski/alert_brokers/alert_broker_turbo.py", \
    "kowalski/alert_brokers/"]


COPY ["kowalski/ingesters/__init__.py", \
    "kowalski/ingesters/ingest_catalog.py", \
    "kowalski/ingesters/ingest_gaia_edr3.py", \
    "kowalski/ingesters/ingest_igaps.py", \
    "kowalski/ingesters/ingest_ps1_strm.py", \
    "kowalski/ingesters/ingest_ptf_matchfiles.py", \
    "kowalski/ingesters/ingest_turbo.py", \
    "kowalski/ingesters/ingest_vlass.py", \
    "kowalski/ingesters/ingest_ztf_alert_aux.py", \
    "kowalski/ingesters/ingest_ztf_matchfiles.py", \
    "kowalski/ingesters/ingest_ztf_public.py", \
    "kowalski/ingesters/ingest_ztf_source_classifications.py", \
    "kowalski/ingesters/ingest_ztf_source_features.py", \
    "kowalski/ingesters/ingester.py", \
    "kowalski/ingesters/"]


COPY kowalski/tests/brokers kowalski/tests/brokers

COPY kowalski/tests/misc kowalski/tests/misc

COPY conf/supervisord_ingester.conf.template conf/

COPY server.properties .

COPY Makefile .

ENV USING_DOCKER=true

# install python libs and generate supervisord config file
RUN source env/bin/activate && \
    uv pip install -r requirements/requirements.txt --no-cache-dir && \
    uv pip install -r requirements/requirements_ingester.txt --no-cache-dir && \
    uv pip install -r requirements/requirements_test.txt --no-cache-dir

# run container
CMD  source env/bin/activate && make run_ingester
