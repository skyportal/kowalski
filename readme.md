# Kowalski: a multi-survey data archive and alert broker for time-domain astronomy

Kowalski is an API-driven multi-survey data archive and alert broker.
Its main focus is the [Zwicky Transient Facility](https://ztf.caltech.edu).

## Technical details

A schematic overview of the functional aspects of `Kowalski` and how they interact is shown below:

![data/img/kowalski.jpg](data/img/kowalski.jpg)

- A non-relational (NoSQL) database `MongoDB` powers the data archive, the alert stream sink,
and the alert handling service.
- An API layer provides an interface for the interaction with the backend:
it is built using a `python` asynchronous web framework, `aiohttp`, and the standard `python` async event loop
serves as a simple, fast, and robust job queue.
Multiple instances of the API service are maintained using the `Gunicorn` WSGI HTTP Server.
- A [programmatic `python` client](https://github.com/dmitryduev/penquins) is also available
to interact with Kowalski's API.
- Incoming and outgoing traffic can be routed through `traefik`,
which acts as a simple and performant reverse proxy/load balancer.
- An alert brokering layer listens to `Kafka` alert streams and uses a `dask.distributed` cluster for
distributed alert packet processing, which includes data preprocessing, execution of machine learning models,
catalog cross-matching, and ingestion into `MongoDB`.
It also executes user-defined filters based on the augmented alert data and posts the filtering results
to a [`SkyPortal`](https://skyportal.io/) instance.
- Kowalski is containerized using `Docker` software and orchestrated with `docker-compose`
allowing for simple and efficient deployment in the cloud and/or on-premise. However, it can also run without Docker especially for development purposes.

## Interacting with a `Kowalski` instance

`Kowalski` is an API-first system. The full OpenAPI specs can be found [here](https://kowalski.caltech.edu/docs/api/). Most users will only need the [queries section](https://kowalski.caltech.edu/docs/api/#tag/queries) of the specs.

The easiest way to interact with a `Kowalski` instance is by using a python client [`penquins`](https://github.com/dmitryduev/penquins).

## Cloning and Environment configuration

Start off by creating your own kowalski fork and github, and cloning it, then `cd` into the cloned directory:

```bash
git clone https://github.com/<your_github_id>/kowalski.git
cd kowalski
```
Make sure you have a `python` environment that meets the requirements to run `Kowalski`. You can use both conda and virtualenv. Using virtualenv, you can do:

```bash
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

## Spin up your own `kowalski` **using Docker**

### Setting up config files

You need config files in order to run `Kowalski`. You can start off by copying the default config/secrets over:

```bash
cp config.defaults.yaml config.yaml
cp docker-compose.defaults.yaml docker-compose.yaml
```

`config.yaml` contains the API and ingester configs, the `supervisord` config for the API and ingester containers,
together with all the secrets, so be careful when committing code / pushing docker images.

However, if you want to run in a production setting, be sure to modify `config.yaml` and choose strong passwords!

`docker-compose.yaml` serves as a config file for `docker-compose`, and can be used for different Kowalski deployment modes.
Kowalski comes with several template `docker-compose` configs (see [below](#different-deployment-scenarios) for more info).

### Building Kowalski

Finally, once you've set the config files, you can build an instance of Kowalski.
You can do this with the following command:

```bash
./kowalski.py up --build
```

You have now successfully built a `Kowalski` instance!
Any time you want to rebuild `kowalski`, you need to re-run this command.

### Interacting with a Kowalski build

If you want to just interact with a `Kowalski` instance that has already been built, you can drop the `--build` flag:

* `./kowalski.py up` to start up a pre-built Kowalski instance
* `./koiwalski.py down`to shut down a pre-built Kowalski instance

### Run tests

You can check that a running docker `Kowalski` instance is working by using the Kowalski test suite:

```bash
./kowalski.py test
```

### Shut down `Kowalski`

```bash
./kowalski.py down
```

## Spin up your own `kowalski` **without Docker** (amd64 only for now)

### Setting up config files

Similar to the Docker setup, you need config files in order to run `Kowalski`. You can start off by copying the default config/secrets over. Here however, the default config file is `config.local.yaml`:

```bash
cp config.local.yaml config.yaml
cp docker-compose.defaults.yaml docker-compose.yaml
```

The difference between `config.local.yaml` and `config.defaults.yaml` is that the former has all the path variables set to the local relative path of the kowalski repo. This is useful if you want to run `Kowalski` without Docker without having to change many path variables in `config.yaml`.

### Setting up the MongoDB database

You will need to edit the `database` section to point to your local mongodb instance, or to a mongodb atlas cluster, in which case you should set `database.srv` to `true`, and `database.replica_set` to the name of your cluster or simply `null`.
If you are using a mongodb atlas cluster, kowalski won't be able to create admin users, so you will need to do so manually on the cluster's web interface. You will need to create 2 users: admin user and user, based on what usernames and passwords you've set in the config file.

If you are running your own MongoDB cluster locally *(not using mongoDB atlas or any remove server)*, it is likely that database.host should be 127.0.0.1 or similar. For simplicity, we also set database.replica_set to null. We also need to set the admin and user roles for the database. To do so, login to mongdb and set (using the default values from the config):

```bash
mongosh --host 127.0.0.1 --port 27017
```
and then from within the mongo terminal

```bash
use kowalski
db.createUser( { user: "mongoadmin", pwd: "mongoadminsecret", roles: [ { role: "userAdmin", db: "admin" } ] } )
db.createUser( { user: "ztf", pwd: "ztf", roles: [ { role: "readWrite", db: "admin" } ] } )
db.createUser( { user: "mongoadmin", pwd: "mongoadminsecret", roles: [ { role: "userAdmin", db: "kowalski" } ] } )
db.createUser( { user: "ztf", pwd: "ztf", roles: [ { role: "readWrite", db: "kowalski" } ] } )
```

### Installing python and system dependencies

This time, you will need to start the different services manually. But first, you'll need to install the python dependencies:

```bash
source env/bin/activate
pip install -r kowalski/requirements_api.txt
pip install -r kowalski/requirements_ingester.txt
pip install -r kowalski/requirements_tools.txt
```

Then, you'll need to install kafka and zookeeper:

```bash
export scala_version=2.13
export kafka_version=3.4.0
wget https://downloads.apache.org/kafka/$kafka_version/kafka_$scala_version-$kafka_version.tgz
tar -xzf kafka_$scala_version-$kafka_version.tgz
```

Installed in this way, path.kafka in the config should be set to ./kafka_2.13-3.4.0.

The last step of the kafka setup is to replace its `kafka_$scala_version-$kafka_version/config/server.properties` file with the one in `kowalski/server.properties`. As this setup is meant to run locally outside docker, you should modify said `server.properties` file to set `log.dirs=/data/logs/kafka-logs` to `log.dirs=./data/logs/kafka-logs` instead (the addition here is the `.` at the beginning of the path) to avoid permission issues.

### Download the ML models

The ingester we will run later on needs the different models to be downloaded. To do so, run:

```bash
cd kowalski && mkdir models && cd models && \
braai_version=d6_m9 && acai_h_version=d1_dnn_20201130 && \
acai_v_version=d1_dnn_20201130 && acai_o_version=d1_dnn_20201130 && \
acai_n_version=d1_dnn_20201130 && acai_b_version=d1_dnn_20201130 && \
wget https://github.com/dmitryduev/braai/raw/master/models/braai_$braai_version.h5 -O braai.$braai_version.h5 && \
wget https://github.com/dmitryduev/acai/raw/master/models/acai_h.$acai_h_version.h5 && \
wget https://github.com/dmitryduev/acai/raw/master/models/acai_v.$acai_v_version.h5 && \
wget https://github.com/dmitryduev/acai/raw/master/models/acai_o.$acai_o_version.h5 && \
wget https://github.com/dmitryduev/acai/raw/master/models/acai_n.$acai_n_version.h5 && \
wget https://github.com/dmitryduev/acai/raw/master/models/acai_b.$acai_b_version.h5
```

### API

The API app can then be run with

```bash
KOWALSKI_APP_PATH=./ KOWALSKI_PATH=kowalski python kowalski/api.py
```

Then tests can be run by going into the kowalski/ directory

```bash
cd kowalski
```

and running:

```bash
KOWALSKI_APP_PATH=../ python -m pytest -s api.py ../tests/test_api.py
```

which should complete.

### Dask cluster

Next, you need to start the dask scheduler and workers. These are the processes that will run the machine learning models and the alert filtering when running the ingester and/or the broker.

Before starting it, you might want to consider lowering or increasing the number of workers and threads per worker in the config file, depending on your config. The default values are set to 4 workers and 4 threads per worker. This means that the dask cluster will be able to process 4 alerts at the same time. If you have a GPU available with a lot of VRAM, you might want to increase the number of workers and threads per worker to take advantage of it.

```bash
cd kowalski
```

and running:

```bash
KOWALSKI_APP_PATH=../ python dask_cluster.py
```

If you have a GPU available, tensorflow might use it by default. However, you might run into some issue running the ml models when following the instructions below if you GPU does not have much memory. To avoid this, you can set the environment variable `CUDA_VISIBLE_DEVICES=-1` before running the dask cluster:

```bash
CUDA_VISIBLE_DEVICES=-1 KOWALSKI_APP_PATH=../ python dask_cluster.py
```

### Alert Broker

If you have access to a ZTF alert stream and have it configured accordingly in the config. If you intend to simulate an alert stream locally, you should change the kafka config to set the `bootstrap.servers` to `localhost:9092`, and `zookeeper` to `localhost:2181`.Then, you can run the broker with
```bash
KOWALSKI_APP_PATH=./ python kowalski/alert_broker_ztf.py
```

**Otherwise, when running locally for testing purposes, you can simply run the tests which will create a mock alert stream and run the broker on it.**

Then tests can be run by going into the kowalski/ directory
*(when running the tests, you do not need to start the broker as instructed in the previous step. The tests will take care of it)*

```bash
cd kowalski
```

and running:

```bash
KOWALSKI_APP_PATH=../ python -m pytest -s alert_broker_ztf.py ../tests/test_alert_broker_ztf.py
```

We also provide an option `USE_TENSORFLOW=False` for users who cannot install Tensorflow for whatever reason.

### Ingester (Pushing alerts to a kafka)

Once the broker is running, you might want to create a local kafka stream of alerts to test it. To do so, you can run the ingester with

```bash
cd kowalski
```

and running:

```bash
PYTHONPATH=. KOWALSKI_APP_PATH=../ python ../tools/kafka_stream.py --topic="<topic_listened_by_your_broker" --path="<path_to_alerts_in_KOWALSKI_APP_PATH/data/>" --test=True
```

where `<topic_listened_by_your_broker>` is the topic listened by your broker (ex: `ztf_20200301_programid3` for the ztf broker) and `<path_to_alerts_in_KOWALSKI_APP_PATH/data/>` is the path to the alerts in the `data/` directory of the kowalski app (ex: `ztf_alerts/20200202` for the ztf broker).

**Otherwise, you can test both ingestion and broker at the same time by running the ingester tests:**

Then tests can be run by going into the kowalski/ directory (similarly to the broker tests, you do not need to star the broker manually as instructed in the previous step. The tests will take care of it). Make sure that the dask cluster is running before running the ingester tests.

```bash
cd kowalski
```

and running:

```bash
PYTHONPATH=. KOWALSKI_APP_PATH=../ python -m pytest ../tests/test_ingester.py
```

The ingester tests can take a while to complete, be patient! If they encounter an error with kafka, it is likely that you did not modify the `server.properties` file as instructed above, meaning that the kafka logs can't be created, which blocks the test without showing an error.

If that happens, you can simply run the ingester without pytest so that you can see the logs and debug the issue:

```bash
PYTHONPATH=. KOWALSKI_APP_PATH=../ python ../tests/test_ingester.py
```

Another common problem is that if you stop the ingester test while its running or if it fails, it might leave a lock file in the kafka logs directory, which will prevent kafka from starting the next time you try to do so. If that happens, you can: delete the lock file, or simply retry starting the test, as a failed test attempt is supposed to remove the lock file. Then, the test should work on the following attempt.

We stronly advise you to open the `server.log` file found in /kakfa_$scala_version-$kafka_version/logs/ to see what is going on with the kafka server. It will be particularly useful if you encounter errors with the ingester tests or the broker.

### Tools

Then tests can be run by going into the kowalski/ directory

```bash
cd kowalski
```

and running:
```bash
KOWALSKI_APP_PATH=../ python -m pytest -s ../tools/istarmap.py ../tests/test_tools.py
```


## Different Deployment scenarios (using Docker)

`Kowalski` uses `docker-compose` under the hood and requires a `docker-compose.yaml` file.
There are several available deployment scenarios:

- Bare-bones
- Bare-bones + broker for `SkyPortal` / `Fritz`
- Behind `traefik`

### Bare-bones

Use `docker-compose.defaults.yaml` as a template for `docker-compose.yaml`.
Note that the environment variables for the `mongo` service must match
`admin_*` under `kowalski.database` in `config.yaml`.

### Bare-bones + broker for [`SkyPortal`](https://skyportal.io/) / [`Fritz`](https://github.com/fritz-marshal/fritz)

Use `docker-compose.fritz.defaults.yaml` as a template for `docker-compose.yaml`.
If you want the alert ingester to post (filtered) alerts to `SkyPortal`, make sure
`{"misc": {"broker": true}}` in `config.yaml`.

### Behind `traefik`

Use `docker-compose.traefik.defaults.yaml` as a template for `docker-compose.yaml`.

If you have a publicly accessible host allowing connections on port `443` and a DNS record with the domain
you want to expose pointing to this host, you can deploy `kowalski` behind [`traefik`](http://traefik.io),
which will act as the edge router -- it can do many things including load-balancing and
getting a TLS certificate from `letsencrypt`.

In `docker-compose.yaml`:
- Replace `kowalski@caltech.edu` with your email.
- Replace `private.caltech.edu` with your domain.

## Docs

OpenAPI specs are to be found under `/docs/api` once `Kowalski` is up and running.

## Developer guide

### How to contribute

Contributions to Kowalski are made through
[GitHub Pull Requests](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests),
a set of proposed commits (or patches).

To prepare, you should:

- Create your own fork the [kowalski repository](https://github.com/dmitryduev/kowalski) by clicking the "fork" button.

- [Set up SSH authentication with GitHub](https://help.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh).

- Clone (download) your copy of the repository, and set up a remote called `upstream` that points to the main Kowalski repository.

  ```sh
  git clone git@github.com:<yourname>/kowalski
  git remote add upstream git@github.com:dmitryduev/kowalski
  ```

Then, for each feature you wish to contribute, create a pull request:

1. Download the latest version of Kowalski, and create a new branch for your work.

   Here, let's say we want to contribute some documentation fixes; we'll call our branch `rewrite-contributor-guide`.

   ```sh
   git checkout master
   git pull upstream master
   git checkout -b rewrite-contributor-guide
   ```

2. Make modifications to Kowalski and commit your changes using `git add` and `git commit`.
Each commit message should consist of a summary line and a longer description, e.g.:

   ```text
   Rewrite the contributor guide

   While reading through the contributor guide, I noticed several places
   in which instructions were out of order. I therefore reorganized all
   sections to follow logically, and fixed several grammar mistakes along
   the way.
   ```

3. When ready, push your branch to GitHub:

   ```sh
   git push origin rewrite-contributor-guide
   ```

   Once the branch is uploaded, GitHub should print a URL for turning your branch into a pull request.
   Open that URL in your browser, write an informative title and description for your pull request, and submit it.
   There, you can also request a review from a team member and link your PR with an existing issue.

4. The team will now review your contribution, and suggest changes.
*To simplify review, please limit pull requests to one logical set of changes.*
To incorporate changes recommended by the reviewers, commit edits to your branch, and push to the branch again
(there is no need to re-create the pull request, it will automatically track modifications to your branch).

5. Sometimes, while you were working on your feature, the `master` branch is updated with new commits, potentially
resulting in conflicts with your feature branch. To fix this, please merge in the latest `upstream/master` branch:

    ```shell script
    git merge rewrite-contributor-guide upstream/master
    ```
Developers may merge `master` into their branch as many times as they want to.

6. Once the pull request has been reviewed and approved by at least two team members, it will be merged into Kowalski.

### Pre-commit hook

Install our pre-commit hook as follows:

```
pip install pre-commit
pre-commit install
```

This will check your changes before each commit to ensure that they
conform with our code style standards. We use `black` to reformat `Python`
code and `flake8` to verify that code complies with [PEP8](https://www.python.org/dev/peps/pep-0008/).


### Add a new alert stream to Kowalski

To add a new alert stream to kowalski, see the [PR](https://github.com/skyportal/kowalski/pull/174) associated with the addition of WINTER to Kowalski.
A brief summary of the changes required (to add WINTER into Kowalski, but hopefully can be extended to any other survey) is given below -
1. A new `kowalski/alert_broker_<winter>.py` needs to be created for the new alert stream. This can be modelled off the existing alert_broker_ztf.py or alert_broker_pgir.py scripts, with the following main changes -

   a. `watchdog` needs to be pointed to pull from the correct topic associated with the new stream

   b. `topic_listener` needs to be updated to use the correct dask-ports associated with the new stream from the config file  (every alert stream should have different dask ports to avoid conflicts). `topic_listener` also needs to be updated to use the `<WNTR>AlertConsumer` asociated with the new stream.

   c. `<WNTR>AlertConsumer` needs to be updated per the requirements of the survey. For example, WINTER does not require MLing prior to ingestion, so that step is excluded unlike in the `ZTFAlertConsumer`. The `WNTRAlertConsumer` also does a cross-match to the ZTF alert stream, a step that is obviously not present in `ZTFAlertConsumer`.

   d. `<WNTR>AlertWorker` needs to be updated to use the correct stream from SkyPortal. `alert_filter__xmatch_ztf_alerts` needs to be updated with the new survey-specific cross-match  radius (2 arcsec for WINTER).

2. In `kowalski/alert_broker.py`, `make_photometry` needs to be updated with the filterlist and zeropoint system appropriate for the new stream.

3. A new `kowalski/dask_cluster_<winter>,py` needs to be created, modeled on `dask_cluster.py` but using the ports for the new stream from the config file.

4. The config file `config.defaults.yaml` needs to be updated to include the collections, upstream filters, crossmatches, dask ports for the new stream. No two streams should use the same ports for dask to avoid conflicts. Entries also need to be made in the `supervisord` section of the config file so that `alert_broker_<winter>.py` and `dask_cluster_<winter>.py` can be run through supervisor.

5. Some alerts need to be added to `data/` for testing. Tests for alert ingestion (`tests/test_ingester_<wntr>.py`) and alert processing (`tests/test_alert_broker_wntr.py`) can be modeled on the ZTF tests, with appropriate changes for the new stream.

6. Need to edit `ingester.Dockerfile` so that all new files are copied into the docker container.

### Add a new ML model to Kowalski

For now, only the ZTF alert stream has a method implement to run ML models on the alerts. However, this can be extended as reused as a basis to run ML models on other streams as well.

To add a new ML model to run on the ZTF alert stream, you simply need to add the model to the `models` directory, and add the model to `ml_models.ZTF` in the config file. The model will then be automatically loaded and run on the alerts.

Here are the exact steps to add a new ML model to Kowalski:

1. Add the model in .h5 format, or if you are using a .pb format you can also add the model files and directories in a folder called `<model_name.model_version>` in the `models` directory.

2. Add the model name to `ml_models.ZTF` in the config file. All models need to have at least the following fields:

   - `triplet`: True or False, whether the model uses the triplet (images) or not as an input to the model
   - `feature_names`: list of features used by the model as a tuple, they need to be a subset of the `ZTF_ALERT_NUMERICAL_FEATURES` found in `kowalski/utils.py`. Ex: `('drb', 'diffmaglim', 'ra', 'dec', 'magpsf', 'sigmapsf')`
   - `version`: version of the model

   Then, you might want to provide additional information about the model, such as:
   - `feature_norms`: dictionary of feature names and their normalization values, if the model was trained with normalized features
   - `order`: in which order do the triplet and features need to be passed to the model. ex: `['triplet', 'features']` or `['features', 'triplet']`
   - `format`: format of the model, either `h5` or `pb`. If not provided, the default is `h5`.

The best way to see if the model is being loaded correctly is to run the broker tests mentioned earlier. These tests will show you the models that are running, and the errors encountered when loading the models (if any).
