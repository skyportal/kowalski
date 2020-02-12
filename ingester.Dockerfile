FROM python:3.7
#FROM python:3.7-slim

# Install vim, git, cron, and jdk
#RUN apt-get update && apt-get -y install apt-file && apt-file update && apt-get -y install vim && \
#    apt-get -y install git && apt-get install -y default-jdk

# Install jdk
RUN apt-get update && apt-get install -y default-jdk

# place to keep our app and the data:
RUN mkdir -p /app /data /data/logs /_tmp /kafka

# Kafka:
ADD http://apache.claz.org/kafka/2.2.0/kafka_2.11-2.2.0.tgz /kafka
#ADD http://apache.claz.org/kafka/2.4.0/kafka_2.12-2.4.0.tgz /kafka
RUN tar -xzf /kafka/kafka_2.11-2.2.0.tgz

# copy over the secrets and the code
#COPY ["secrets.json", "kowalski/*_ingester.*", "kowalski/utils.py",\
#      "kowalski/alert_watcher_ztf.py",\
#      "tests/test_ingester.py",\
#      "/app/"]
COPY ["secrets.json", "kowalski/*_ingester.*", "kowalski/utils.py",\
      "/app/"]

# change working directory to /app
WORKDIR /app

# install python libs and generate keys
RUN pip install -r /app/requirements_ingester.txt --no-cache-dir

COPY kowalski/alert_watcher_ztf.py /app/

# run tests
#RUN python -m pytest -s test_ingester.py

# run container
CMD /usr/local/bin/supervisord -n -c supervisord_ingester.conf
#CMD python api.py
