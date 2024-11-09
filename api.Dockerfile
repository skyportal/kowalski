FROM debian:bookworm-slim

WORKDIR /kowalski

# place to keep our app and the data:
RUN mkdir -p data logs /_tmp

SHELL ["/bin/bash", "-c"]

ENV VIRTUAL_ENV=/usr/local

RUN apt-get update && apt-get install -y curl wget && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    apt-get install -y build-essential libssl-dev libffi-dev python3-dev cargo

ADD https://astral.sh/uv/install.sh /uv-installer.sh

RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

RUN uv venv env --python=python3.10

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
        "kowalski/tools/install_python_requirements.py", \
        "kowalski/tools/watch_logs.py", \
        "kowalski/tools/gcn_utils.py", \
        "kowalski/tools/"]

COPY kowalski/tests/api kowalski/tests/api

COPY conf/supervisord_api.conf.template conf/

COPY Makefile .

RUN source env/bin/activate && \
    uv pip install packaging setuptools && \
    uv pip install -r requirements/requirements.txt && \
    uv pip install -r requirements/requirements_test.txt && \
    uv pip install -r requirements/requirements_api.txt

# remove cached files
RUN rm -rf $HOME/.cache/uv

# run container
CMD ["/bin/bash", "-c", "source env/bin/activate && make run_api"]
