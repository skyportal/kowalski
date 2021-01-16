# Kowalski: enhancing time-domain astronomy

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

Clone the repo and cd to the cloned directory:
```bash
git clone https://github.com/dmitryduev/kowalski.git
cd kowalski
```

Use the `kowalski.py` utility to manage `Kowalski`.

Make sure the requirements to run the `kowalski.py` utility are met, e.g.:

```bash
pip install -r requirements.txt
```

Start up `Kowalski` using the default config/secrets (copying them over):

```bash
./kowalski.py up
```

### Config file

You should `cp config.defaults.yaml config.yaml` instead of using the default config in a production setting.
Make sure to choose strong passwords!

`config.yaml` contains the API and ingester configs, the `supevisord` config for the API and ingester containers,
together with all the secrets, so be careful when committing code / pushing docker images.


### Deployment scenarios

```bash
./kowalski.py up
```

`Kowalski` uses `docker-compose` under the hood and requires a `docker-compose.yaml` file.
There are several available deployment scenarios:

- Bare-bones
- Bare-bones + broker for `SkyPortal` / `Fritz`
- Behind `traefik`

#### Bare-bones

Use `docker-compose.defaults.yaml` as a template for `docker-compose.yaml`.
Note that the environment variables for the `mongo` service must match
`admin_*` under `kowalski.database` in `config.yaml`.

#### Bare-bones + broker for [`SkyPortal`](https://skyportal.io/) / [`Fritz`](https://github.com/fritz-marshal/fritz)

Use `docker-compose.fritz.defaults.yaml` as a template for `docker-compose.yaml`.
If you want the alert ingester to post (filtered) alerts to `SkyPortal`, make sure
`{"misc": {"broker": true}}` in `config.yaml`.

#### Behind `traefik`

Use `docker-compose.traefik.defaults.yaml` as a template for `docker-compose.yaml`.

If you have a publicly accessible host allowing connections on port `443` and a DNS record with the domain
you want to expose pointing to this host, you can deploy `kowalski` behind [`traefik`](http://traefik.io),
which will act as the edge router -- it can do many things including load-balancing and
getting a TLS certificate from `letsencrypt`.

In `docker-compose.yaml`:
- Replace `kowalski@caltech.edu` with your email.
- Replace `private.caltech.edu` with your domain.


## Run tests

```bash
./kowalski.py test
```

## Shut down `Kowalski`

```bash
./kowalski.py down
```

## Docs

OpenAPI specs are to be found under `/docs/api` once `Kowalski` is up and running.

## Developer notes

### Pre-commit hook

Install our pre-commit hook as follows:

```
pip install pre-commit
pre-commit install
```

This will check your changes before each commit to ensure that they
conform with our code style standards. We use black to reformat Python
code.
