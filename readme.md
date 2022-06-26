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
allowing for simple and efficient deployment in the cloud and/or on-premise.

## Interacting with a `Kowalski` instance

`Kowalski` is an API-first system. The full OpenAPI specs can be found [here](https://kowalski.caltech.edu/docs/api/). Most users will only need the [queries section](https://kowalski.caltech.edu/docs/api/#tag/queries) of the specs.

The easiest way to interact with a `Kowalski` instance is by using a python client [`penquins`](https://github.com/dmitryduev/penquins).


## Spin up your own `kowalski`

### Cloning and Environment configuration

Start off by cloning the repo, then `cd` into the cloned directory:
```bash
git clone https://github.com/dmitryduev/kowalski.git
cd kowalski
```
Make sure you have a `python` environment that meets the requirements to run `Kowalski`:

```bash
pip install -r requirements.txt
```

You can then use the `kowalski.py` utility to manage `Kowalski`.

### Setting up config files

You need config files in order to run `Kowalski`. You can start off by copying the default config/secrets over:

```bash
cp config.defaults.yaml config.yaml
cp docker-compose.defaults.yaml docker-compose.yaml
```

`config.yaml` contains the API and ingester configs, the `supevisord` config for the API and ingester containers,
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

## Run tests

You can check that a running `Kowalski` instance is working by using the Kowalski test suite:

```bash
./kowalski.py test
```

## Different Deployment scenarios

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

## Shut down `Kowalski`

```bash
./kowalski.py down
```

## Docs

OpenAPI specs are to be found under `/docs/api` once `Kowalski` is up and running.

## Developer guidelines

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


## Building and deploying locally

When developing, it can be useful to just run kowalski directly.

### API

To install the API requirements, run:

```bash
pip install -r kowalski/requirements_api.txt
```

Just as described above, the config file must be created:

```bash
cp config.defaults.yaml config.yaml
```

When running locally, it is likely that database.host should be 127.0.0.1 or similar. For simplicity, we also set database.replica_set to null.

We need to set the admin and user roles for the database. To do so, login to mongdb and set (using the default values from the config):

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

### Alert Broker

To install the broker requirements, run:

```bash
pip install -r kowalski/requirements_ingester.txt
```

The ingester requires kafka, which can be installed with:

```bash
export kafka_version=2.13-2.5.0
wget https://storage.googleapis.com/ztf-fritz/kafka_$kafka_version.tgz
tar -xzf kafka_$kafka_version.tgz
```

Installed in this way, path.kafka in the config should be set to ./kafka_2.13-2.5.0.

The broker can then be run with
```bash
KOWALSKI_APP_PATH=./ python kowalski/alert_broker_ztf.py
```

Then tests can be run by going into the kowalski/ directory

```bash
cd kowalski
```

and running:

```bash
KOWALSKI_APP_PATH=../ KOWALSKI_DATA_PATH=../data python -m pytest -s alert_broker_ztf.py ../tests/test_alert_broker_ztf.py
```

We also provide an option `USE_TENSORFLOW=False` for users who cannot install Tensorflow for whatever reason.

### Ingester

Coming soon.
