FROM python:3.7
#FROM python:3.7-slim

RUN apt-get update

# place to keep our app and the data:
RUN mkdir -p /app /data /data/logs /_tmp

# copy over the secrets and the code
#COPY ["secrets.json", "kowalski/*_ingester.*", "kowalski/utils.py",\
#      "kowalski/alert_watcher_ztf.py",\
#      "/app/"]
COPY ["secrets.json", "kowalski/*_ingester.*", "kowalski/utils.py",\
      "/app/"]

# change working directory to /app
WORKDIR /app

# install python libs and generate keys
RUN pip install -r /app/requirements_ingester.txt --no-cache-dir

COPY kowalski/alert_watcher_ztf.py /app/

# run tests
#RUN python -m pytest -s alert_watcher_ztf.py

# run container
CMD /usr/local/bin/supervisord -n -c supervisord_ingester.conf
#CMD python api.py
