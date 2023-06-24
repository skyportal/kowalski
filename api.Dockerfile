FROM python:3.10
#FROM python:3.7-slim

RUN apt-get update

# place to keep our app and the data:
RUN mkdir -p /kowalski /kowalski/data /kowalski/logs /_tmp

WORKDIR /kowalski

COPY requirements/ requirements/

COPY config.defaults.yaml config.defaults.yaml
COPY docker.yaml config.yaml
COPY version.txt .
COPY data/events/ data/events/
COPY data/schema/ data/schema/

COPY ["kowalski/__init__.py", \
        "kowalski/utils.py", \
        "kowalski/config.py", \
        "kowalski/log.py", \
        "kowalski/"]

COPY ["kowalski/api/__init__.py", \
        "kowalski/api/api.py", \
        "kowalski/api/middlewares.py", \
        "kowalski/api/components_api.yaml", \
        "kowalski/api/"]

COPY ["kowalski/tools/__init__.py", \
        "kowalski/tools/check_app_environment.py", \
        "kowalski/tools/generate_supervisord_conf.py", \
        "kowalski/tools/pip_install_requirements.py", \
        "kowalski/tools/watch_logs.py", \
        "kowalski/tools/gcn_utils.py", \
        "kowalski/tools/"]

COPY kowalski/tests/test_api.py kowalski/tests/

COPY conf/supervisord_api.conf.template conf/

COPY Makefile .


# upgrade pip
RUN pip install --upgrade pip

# install python libs and generate supervisord config file
RUN pip install -r requirements/requirements.txt --no-cache-dir && \
    pip install -r requirements/requirements_api.txt --no-cache-dir

# run container
CMD make run_api
