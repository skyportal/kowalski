# Kowalski

Enhancing time-domain astronomy with the [Zwicky Transient Facility](https://ztf.caltech.edu).

## Ineract with a `Kowalski` instance

`Kowalski` is an API-first system. The full OpenAPI specs can be found [here](https://kowalski.caltech.edu/docs/api/). Most users will only need the [queries section](https://kowalski.caltech.edu/docs/api/#tag/queries) of the specs.

The easiest way to interact with a `Kowalski` instance is by using a python client [`penquins`](https://github.com/dmitryduev/penquins).


## Spin up your own `kowalski`

Clone the repo and cd to the cloned directory:
```bash
git clone https://github.com/dmitryduev/kowalski.git
cd kowalski
```

Use the `kowalski.py` utility to manage `kowalski`.

Make sure the requirements to run the `kowalski.py` utility are met, e.g.:

```bash
pip install -r requirements.txt
```

Start up `kowalski` using the default configs and secrets (copying them over):

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
- Bare-bones alongside locally-running `SkyPortal`
- Behind `traefik`

#### Bare-bones

Use `docker-compose.defaults.yaml` as a template for `docker-compose.yaml`. 
Note that the environment variables for the `mongo` service must match 
`admin_*` under `kowalski.database` in `config.yaml`.

#### Bare-bones alongside locally-running [`SkyPortal`](https://skyportal.io/)

Use `docker-compose.fritz.defaults.yaml` as a template for `docker-compose.yaml`.
If you want the alert ingester to post (filtered) alerts to `SkyPortal`, make sure 
`{"misc": {"post_to_skyportal": true}}` in `config.yaml`.

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

## Shut down `kowalski`

```bash
./kowalski.py down
```

## Docs

OpenAPI specs are to be found under `/docs/api` once `kowalski` is up and running.
