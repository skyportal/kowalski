# kowalski

Enhancing time-domain astronomy with the [Zwicky Transient Facility](https://ztf.caltech.edu).

## Spin up kowalski

Clone the repo and cd to the cloned directory:
```bash
git clone https://github.com/dmitryduev/kowalski-dev.git kowalski
cd kowalski
```

Create `secrets.json` with the secrets. Come up with strong passwords!
```json
{
  "server" : {
    "admin_username": "admin",
    "admin_password": "admin"
  },

  "database": {
    "admin_username": "mongoadmin",
    "admin_password": "mongoadminsecret",
    "username": "ztf",
    "password": "ztf"
  },

  "kafka": {
    "bootstrap.servers": "192.168.0.64:9092,192.168.0.65:9092,192.168.0.66:9092",
    "zookeeper": "192.168.0.64:2181",
    "bootstrap.test.servers": "localhost:9092",
    "zookeeper.test": "localhost:2181"
  },
  
  "skyportal": {
    "host": "0.0.0.0",
    "port": 443,
    "username": "kowalski",
    "password": "password"
  },

  "ztf_depot": {
    "username": "username",
    "password": "password"
  },

  "ztf_ops": {
    "url": "http://site/allexp.tbl",
    "username": "username",
    "password": "password"
  }
}
```

Copy `docker-compose.yaml` to e.g. `docker-compose.deploy.yaml` and change the environment variables for `mongo` 
to match `admin_*` under `database` in `secrets.json`:
```bash
cp docker-compose.yaml docker-compose.deploy.yaml
```

### docker-compose

Run `docker-compose` to fire up `kowalski`:
```bash
docker-compose -f docker-compose.deploy.yaml up --build -d
```

Shut down `kowalski`:
```bash
docker-compose down
```

### docker-compose: deploying behind `traefik`

If you have a publicly accessible host allowing connections on port `443` and a DNS record with the domain 
you want to expose pointing to this host, you can deploy `kowalski` behind [`traefik`](http://traefik.io), 
which will act as the edge router -- it can do many things including load-balancing and 
getting a TLS certificate from `letsencrypt`. 

In `docker-compose.traefik.yaml`:
- Replace `kowalski@caltech.edu` with your email.
- Replace `private.caltech.edu` with your domain.

Spin up `kowalski` behind `traefik`:

```bash
docker-compose -f docker-compose.traefik.yaml up -d
```

### kubernetes

Use [`kompose`](https://kompose.io/). 
It will try to upload images to your space on Docker Hub 
so you need to replace `dmitryduev` with your docker username in `docker-compose.deploy.yaml`.

To create services and deployments for k8s:
```bash
kompose -f docker-compose.deploy.yaml convert
kubectl create -f ...
```

Alternatively, simply do:
```bash
kompose -f docker-compose.deploy.yaml up
```


## Run tests

Ingester:
```bash
docker exec -it kowalski_ingester_1 python -m pytest -s test_ingester.py
```

API:
```bash
docker exec -it kowalski_api_1 python -m pytest -s test_api.py
```

`TODO:` The first test ingests 11 (real!) test alerts. Try out a few queries:

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

## Miscellaneous

### Filtering for `fritz`

`TODO:` Upon alert ingestion into the database, Kowalski can execute user-defined filters and report matches to `fritz`.
This is implemented as an aggregation pipeline that first:
- Selects the newly ingested alert from the `ZTF_alerts` collection by its `candid`
- Removes the cutouts to reduce traffic
- Joins the alert by its `objectId` with the corresponding entry in the `ZTF_alerts_aux` collection containing the 
cross-matches and archival photometry

The user-defined stages come after that. In the example below, an alert gets selected if it has a high `drb` score and
it has no match with the `CLU_20190625` catalog.

```json
{
    "query_type": "aggregate",
    "query": {
        "catalog": "ZTF_alerts",
        "pipeline": [
            {
                "$match": {
                    "candid": 1127561440015015001
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
            	"$match": {
            		"candidate.drb": {
            			"$gt": 0.9
            		},
            		"cross_matches.CLU_20190625.0": {
	            		"$exists": false
	            	}
            	}
            }
        ]
    },
    "kwargs": {
        "max_time_ms": 10
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