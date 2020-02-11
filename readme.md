Run `docker-compose` to start the service:
```bash
docker-compose up --build -d
```

Build and run Kafka producer for testing:
```bash
docker build --rm -t kafka_producer:latest -f kafka-producer.Dockerfile .
docker run -it --rm --name kafka_producer -p 2181:2181 -p 9092:9092 -v data:/data kafka_producer:latest
docker exec -it kafka_producer /bin/bash
``` 