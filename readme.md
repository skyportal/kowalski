# kowalski



## Spin up kowalski

Run `docker-compose` to fire up `kowalski`:
```bash
docker-compose up --build -d
```

Shut down `kowalski`:
```bash
docker-compose down
```

## Run tests

API:
```bash
docker exec -it kowalski_app_1 /bin/bash
python -m pytest -s test_api.py
```

Ingester:
```bash
docker exec -it kowalski_ingester_1 /bin/bash
python -m pytest -s test_ingester.py
```

## Miscellaneous

Build and run a dedicated container for the Kafka producer (for testing):
```bash
docker build --rm -t kafka_producer:latest -f kafka-producer.Dockerfile .
docker run -it --rm --name kafka_producer -p 2181:2181 -p 9092:9092 kafka_producer:latest
docker exec -it kafka_producer /bin/bash
``` 