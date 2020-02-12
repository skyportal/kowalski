Run `docker-compose` to fire up Kowalski:
```bash
docker-compose up --build -d
```

Shut down Kowalski:
```bash
docker-compose down
```

---

Build and run a dedicated container for the Kafka producer (for testing):
```bash
docker build --rm -t kafka_producer:latest -f kafka-producer.Dockerfile .
docker run -it --rm --name kafka_producer -p 2181:2181 -p 9092:9092 kafka_producer:latest
#docker run -it --rm --name kafka_producer -p 2181:2181 -p 9092:9092 -v /Users/dmitryduev/_caltech/code/kowalski/data/:/data/ kafka_producer:latest
docker exec -it kafka_producer /bin/bash
``` 