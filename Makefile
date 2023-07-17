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

init_kafka:
	$(PYTHON) kowalski/tools/init_kafka.py

run: setup_all init_models init_kafka
	$(SUPERVISORD)

run_macos : setup_all_macos init_models init_kafka
	$(SUPERVISORD)

run_api: setup_api
	$(SUPERVISORD_API)

run_ingester: setup_ingester init_models init_kafka
	$(SUPERVISORD_INGESTER)

run_ingester_macos: setup_ingester_macos init_models init_kafka
	$(SUPERVISORD_INGESTER)

monitor:
	$(SUPERVISORCTL) -i

monitor_api:
	$(SUPERVISORCTL_API) -i

monitor_ingester:
	$(SUPERVISORCTL_INGESTER) -i

test: setup_all init_models init_kafka
	$(PYTHON) kowalski/tools/tests.py

test_macos: setup_all_macos init_models init_kafka
	$(PYTHON) kowalski/tools/tests.py

docker_setup: setup
	$(PYTHON) kowalski/tools/docker.py setup $(FLAGS)

docker_up: docker_setup
	$(PYTHON) kowalski/tools/docker.py up $(FLAGS)

docker_down: docker_setup
	$(PYTHON) kowalski/tools/docker.py down $(FLAGS)

docker_build: docker_setup
	$(PYTHON) kowalski/tools/docker.py build $(FLAGS)

docker_test: docker_setup
	$(PYTHON) kowalski/tools/tests.py --use_docker

docker_seed : docker_setup
	$(PYTHON) kowalski/tools/docker.py seed $(FLAGS)

log: ## Monitor log files for all services.
log: setup
	$(PYTHON) kowalski/tools/watch_logs.py

stop: ## Stop all services in supervisord.
stop: setup
	$(SUPERVISORCTL) stop all

stop_ingester: ## Stop all services in supervisord.
stop_ingester: setup
	$(SUPERVISORCTL_INGESTER) stop all

stop_api: ## Stop all services in supervisord.
stop_api: setup
	$(SUPERVISORCTL_API) stop all
