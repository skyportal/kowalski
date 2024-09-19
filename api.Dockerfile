FROM python:3.10
#FROM python:3.7-slim

RUN apt-get update && \
    apt-get install -y curl && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    apt-get install -y build-essential && \
    apt-get install -y libssl-dev && \
    apt-get install -y libffi-dev && \
    apt-get install -y python3-dev && \
    apt-get install -y cargo

ENV PATH="/root/.cargo/bin:${PATH}"

# place to keep our app and the data:
RUN mkdir -p /kowalski /kowalski/data /kowalski/logs /_tmp

WORKDIR /kowalski

COPY requirements/ requirements/

COPY config.defaults.yaml config.defaults.yaml
COPY docker.yaml config.yaml
COPY version.txt .
COPY schema/ schema/

COPY ["kowalski/__init__.py", \
        "kowalski/utils.py", \
        "kowalski/config.py", \
        "kowalski/log.py", \
        "kowalski/"]

COPY kowalski/api kowalski/api

COPY ["kowalski/tools/__init__.py", \
        "kowalski/tools/check_app_environment.py", \
        "kowalski/tools/generate_supervisord_conf.py", \
        "kowalski/tools/pip_install_requirements.py", \
        "kowalski/tools/watch_logs.py", \
        "kowalski/tools/gcn_utils.py", \
        "kowalski/tools/"]

COPY kowalski/tests/api kowalski/tests/api

COPY conf/supervisord_api.conf.template conf/

COPY Makefile .


# upgrade pip
RUN pip install --upgrade pip

# install python libs and generate supervisord config file
RUN pip install packaging --no-cache-dir && \
    pip install -r requirements/requirements.txt --no-cache-dir && \
    pip install -r requirements/requirements_api.txt --no-cache-dir && \
    pip install -r requirements/requirements_test.txt --no-cache-dir

# run container
CMD make run_api
