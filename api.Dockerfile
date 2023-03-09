FROM python:3.10
#FROM python:3.7-slim

RUN apt-get update

# place to keep our app and the data:
RUN mkdir -p /kowalski /kowalski/data /kowalski/logs /_tmp

WORKDIR /kowalski

COPY requirements_api.txt .

COPY config.yaml .
COPY version.txt .

COPY kowalski/__init__.py kowalski/
COPY kowalski/utils.py kowalski/

COPY kowalski/api/__init__.py kowalski/api/
COPY kowalski/api/api.py kowalski/api/
COPY kowalski/api/middlewares.py kowalski/api/
COPY kowalski/api/components_api.yaml kowalski/api/

COPY kowalski/tools/__init__.py kowalski/tools/
COPY kowalski/tools/generate_supervisord_conf.py kowalski/tools/

COPY kowalski/tests/test_api.py kowalski/tests/

# upgrade pip
RUN pip install --upgrade pip

# install python libs and generate supervisord config file
RUN pip install -r requirements_api.txt --no-cache-dir && \
    python kowalski/tools/generate_supervisord_conf.py api

# run container
CMD /usr/local/bin/supervisord -n -c supervisord_api.conf
