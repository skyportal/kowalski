FROM python:3.10

WORKDIR /kowalski

RUN apt-get update && \
    apt-get install -y curl && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    apt-get install -y build-essential && \
    apt-get install -y libssl-dev && \
    apt-get install -y libffi-dev && \
    apt-get install -y python3-dev && \
    apt-get install -y cargo

ENV VIRTUAL_ENV=/usr/local
ENV PATH="/root/.cargo/bin:${PATH}"

SHELL ["/bin/bash", "-c"]

# place to keep our app and the data:
RUN mkdir -p data logs /_tmp

RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    uv venv env --python=python3.10

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

# install python libs and generate supervisord config file
RUN source env/bin/activate && \
    uv pip install -r requirements/requirements.txt --no-cache-dir && \
    uv pip install -r requirements/requirements_api.txt --no-cache-dir && \
    uv pip install -r requirements/requirements_test.txt --no-cache-dir

# run container
CMD ["/bin/bash", "-c", "source env/bin/activate && make run_api"]
