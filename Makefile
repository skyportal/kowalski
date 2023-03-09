SHELL = /bin/bash

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

generate_supervisord_conf:
	$(PYTHON) kowalski/tools/generate_supervisord_conf.py all

generate_supervisord_api_conf:
	$(PYTHON) kowalski/tools/generate_supervisord_conf.py api

generate_supervisord_ingester_conf:
	$(PYTHON) kowalski/tools/generate_supervisord_conf.py ingester

init_models:
	$(PYTHON) kowalski/tools/init_models.py

run: generate_supervisord_conf init_models
	$(SUPERVISORD)

run_api: generate_supervisord_api_conf
	$(SUPERVISORD_API)

run_ingester: generate_supervisord_ingester_conf init_models
	$(SUPERVISORD_INGESTER)

monitor:
	$(SUPERVISORCTL) status

monitor_api:
	$(SUPERVISORCTL_API) status

monitor_ingester:
	$(SUPERVISORCTL_INGESTER) status
