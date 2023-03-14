SHELL = /bin/bash

.DEFAULT_GOAL := help

# Use `config.yaml` by default, unless overridden by user
# through setting FLAGS environment variable
FLAGS:=$(if $(FLAGS),$(FLAGS),)


PYTHON=PYTHONPATH=. python

SUPERVISORD_CFG=conf/supervisord.conf
SUPERVISORD_API_CFG=conf/supervisord_api.conf
SUPERVISORD_INGESTER_CFG=conf/supervisord_ingester.conf

SUPERVISORD=$(PYTHON) -m supervisor.supervisord -n -c $(SUPERVISORD_CFG)
SUPERVISORD_API=$(PYTHON) -m supervisor.supervisord -n -c $(SUPERVISORD_API_CFG)
SUPERVISORD_INGESTER=$(PYTHON) -m supervisor.supervisord -n -c $(SUPERVISORD_INGESTER_CFG)

SUPERVISORCTL=$(PYTHON) -m supervisor.supervisorctl -c $(SUPERVISORD_CFG)
SUPERVISORCTL_API=$(PYTHON) -m supervisor.supervisorctl -c $(SUPERVISORD_API_CFG)
SUPERVISORCTL_INGESTER=$(PYTHON) -m supervisor.supervisorctl -c $(SUPERVISORD_INGESTER_CFG)

# SYSTEM DEPENDENCIES
system_dependencies:
	$(PYTHON) kowalski/tools/check_app_environment.py

# DEPENDENCIES
python_dependencies:
	$(PYTHON) kowalski/tools/pip_install_requirements.py requirements/requirements.txt

python_dependencies_api:
	$(PYTHON) kowalski/tools/pip_install_requirements.py requirements/requirements_api.txt

python_dependencies_ingester:
	$(PYTHON) kowalski/tools/pip_install_requirements.py requirements/requirements_ingester.txt

python_dependencies_ingester_macos:
	$(PYTHON) kowalski/tools/pip_install_requirements.py requirements/requirements_ingester_macos.txt

# SUPERVISORD
generate_supervisord_conf:
	$(PYTHON) kowalski/tools/generate_supervisord_conf.py all

generate_supervisord_api_conf:
	$(PYTHON) kowalski/tools/generate_supervisord_conf.py api

generate_supervisord_ingester_conf:
	$(PYTHON) kowalski/tools/generate_supervisord_conf.py ingester


# SETUP
setup: system_dependencies python_dependencies

setup_api: system_dependencies python_dependencies_api generate_supervisord_api_conf

setup_ingester: system_dependencies python_dependencies_ingester generate_supervisord_ingester_conf

setup_ingester_macos: system_dependencies python_dependencies_ingester_macos generate_supervisord_ingester_conf

setup_all: setup python_dependencies_api python_dependencies_ingester generate_supervisord_conf

setup_all_macos: setup python_dependencies_api python_dependencies_ingester_macos generate_supervisord_conf

init_models:
	$(PYTHON) kowalski/tools/init_models.py

run: setup_all init_models
	$(SUPERVISORD)

run_macos : setup_all_macos init_models
	$(SUPERVISORD)

run_api: setup_api
	$(SUPERVISORD_API)

run_ingester: setup_ingester init_models
	$(SUPERVISORD_INGESTER)

run_ingester_macos: setup_ingester_macos init_models
	$(SUPERVISORD_INGESTER)

monitor:
	$(SUPERVISORCTL) status

monitor_api:
	$(SUPERVISORCTL_API) status

monitor_ingester:
	$(SUPERVISORCTL_INGESTER) status

test: setup_all init_models
	$(PYTHON) kowalski/tools/tests.py

test_macos: setup_all_macos init_models
	$(PYTHON) kowalski/tools/tests.py

docker_up: setup
	$(PYTHON) ./kowalski/tools/docker.py up $(FLAGS)

docker_down: setup
	$(PYTHON) ./kowalski/tools/docker.py down $(FLAGS)

docker_build: setup
	$(PYTHON) ./kowalski/tools/docker.py build $(FLAGS)

docker_test: setup
	$(PYTHON) kowalski/tools/tests.py --use_docker

docker_seed : setup
	$(PYTHON) ./kowalski/tools/docker.py seed $(FLAGS)