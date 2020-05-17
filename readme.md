# kowalski

Enhancing time-domain astronomy with the [Zwicky Transient Facility](https://ztf.caltech.edu).

## Spin up `kowalski`

Clone the repo and cd to the cloned directory:
```bash
git clone https://github.com/dmitryduev/kowalski-dev.git kowalski
cd kowalski
```

Use the `kowalski.py` utility to manage `kowalski`.

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

`Kowalski` uses `docker-compose` under the hood. There are several available deployment scenarios:

- Bare-bones
- Bare-bones alongside locally-running `SkyPortal`
- Behind `traefik`
- todo: `k8s`

#### Bare-bones

```bash
./kowalski.py up
```

Uses `docker-compose.yaml`. Note that the environment variables for the `mongo` service must match 
`admin_*` under `database` in `secrets.json`

#### Bare-bones alongside locally-running [`SkyPortal`](https://skyportal.io/)

```bash
./kowalski.py up --fritz
```

If you want the alert ingester to post (filtered) alerts to `SkyPortal`, make sure 
`{"misc": {"post_to_skyportal": true}}` in `kowalski/config_ingester.json`

#### Behind `traefik`

```bash
./kowalski.py up --traefik
```

If you have a publicly accessible host allowing connections on port `443` and a DNS record with the domain 
you want to expose pointing to this host, you can deploy `kowalski` behind [`traefik`](http://traefik.io), 
which will act as the edge router -- it can do many things including load-balancing and 
getting a TLS certificate from `letsencrypt`. 

In `docker-compose.traefik.yaml`:
- Replace `kowalski@caltech.edu` with your email.
- Replace `private.caltech.edu` with your domain.

### todo: kubernetes

Use [`kompose`](https://kompose.io/). 
It will try to upload images to your space on Docker Hub 
so you need to replace `dmitryduev` with your docker username in `docker-compose.yaml`.

To create services and deployments for k8s:
```bash
kompose -f docker-compose.yaml convert
kubectl create -f ...
```

Alternatively, simply do:
```bash
kompose -f docker-compose.yaml up
```


## Run tests

```bash
./kowalski.py test
```

## API docs

OpenAPI specs are to be found under `/docs/api` once `kowalski` is up and running.

---

## Miscellaneous

### Query `kowalski` using the API

The first test in the test suite ingests some real alerts. Try out a few queries on them:

`/api/auth`

Headers:
```json
{"Content-Type": "application/json"}
```

Body:
```json
{
    "username": "admin",
    "password": "admin"
}
```

Using `curl`:
```bash
curl -d '{"username":"admin", "password":"admin"}' -H "Content-Type: application/json" -X POST https://localhost:4000/api/auth
```

---

`/api/queries`

Headers:
```json
{"Authorization": "Bearer <TOKEN>", "Content-Type": "application/json"}
```

Body:

```json
{
    "query_type": "find",
    "query": {
        "catalog": "ZTF_alerts",
    	"filter": {"classifications.braai": {"$gt": 0.9}},
    	"projection": {"_id": 0, "candid": 1, "classifications.braai": 1}
    }
}
```

```json
{
  "query_type": "cone_search",
  "query": {
    "object_coordinates": {
      "cone_search_radius": 2,
      "cone_search_unit": "arcsec",
      "radec": {"object1": [71.6577756, -10.2263957]}
    },
    "catalogs": {
      "ZTF_alerts": {
        "filter": {},
        "projection": {"_id": 0, "candid": 1, "objectId": 1}
      }
    }
  },
  "kwargs": {
    "filter_first": false
  }
}
```

### Filtering for `fritz`

`TODO:` Upon alert ingestion into the database, Kowalski can execute user-defined filters and report matches to `fritz`.
This is implemented as an aggregation pipeline that first:
- Selects the newly ingested alert from the `ZTF_alerts` collection by its `candid`
- Removes the cutouts to reduce traffic
- Joins the alert by its `objectId` with the corresponding entry in the `ZTF_alerts_aux` collection containing the 
cross-matches and archival photometry

The user-defined stages come after that. In the example below, an alert gets selected if it has a high `drb` score and
it has no match with the `CLU_20190625` catalog.

`TODO:` Annotations as another pipeline stage.

```json
{
    "query_type": "aggregate",
    "query": {
        "catalog": "ZTF_alerts",
        "pipeline": [
            {
                "$match": {
                    "candid": 1127561440015015001,
                    "candidate.programid": {"$in": [0, 1, 2, 3]}
                }
            },
            {
                "$project": {
                    "cutoutScience": 0,
                    "cutoutTemplate": 0,
                    "cutoutDifference": 0
                }
            },
            {
                "$lookup": {
                    "from": "ZTF_alerts_aux",
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            },
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            {
                                "$arrayElemAt": [
                                    "$aux",
                                    0
                                ]
                            },
                            "$$ROOT"
                        ]
                    }
                }
            },
            {
                "$project": {
                    "cross_matches": 1,
                    "prv_candidates": {
                        "$filter": {
                            "input": "$prv_candidates",
                            "as": "item",
                            "cond": {"$in": ["$$item.programid", [1, 2, 3]]}
                        }
                    },
                    "schemavsn": 1,
                    "publisher": 1,
                    "objectId": 1,
                    "candid": 1,
                    "candidate": 1,
                    "classifications": 1,
                    "coordinates": 1
                }
            },
            {
            	"$match": {
            		"candidate.drb": {
            			"$gt": 0.9
            		},
            		"cross_matches.CLU_20190625.0": {
	            		"$exists": false
	            	}
            	}
            },
            {
                "$addFields": {
                    "annotations.author": "dd",
                    "annotations.mean_rb": {"$avg": "$prv_candidates.rb"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "candid": 1,
                    "objectId": 1,
                    "annotations": 1
                }
            }
        ]
    },
    "kwargs": {
        "max_time_ms": 100
    }
}
```

### Kafka producer container

Build and run a dedicated container for the Kafka producer (for testing):
```bash
docker build --rm -t kafka_producer:latest -f kafka-producer.Dockerfile .
docker run -it --rm --name kafka_producer -p 2181:2181 -p 9092:9092 kafka_producer:latest
docker exec -it kafka_producer /bin/bash
``` 