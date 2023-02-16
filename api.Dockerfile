FROM python:3.8
#FROM python:3.7-slim

RUN apt-get update

# place to keep our app and the data:
RUN mkdir -p /app /data /data/logs /_tmp

# copy over the config and the code
COPY ["config.yaml", "version.txt", "kowalski/generate_supervisord_conf.py",\
      "kowalski/middlewares.py", "kowalski/utils.py",\
      "kowalski/components_api.yaml", "kowalski/api.py", "kowalski/requirements_api.txt", "tests/test_api.py",\
      "/app/"]

# change working directory to /app
WORKDIR /app

# install python libs and generate supervisord config file
RUN pip install --upgrade pip
RUN pip install -r /app/requirements_api.txt --no-cache-dir && \
    python generate_supervisord_conf.py api

# run container
#CMD python api.py
CMD /usr/local/bin/supervisord -n -c supervisord_api.conf
