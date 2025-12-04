```bash
docker exec -it kafka-wsl_kafka_1   kafka-topics --create --topic test-topic --bootstrap-server localhost:9092
docker exec -it kafka-wsl_ksqldb-cli_1 ksql http://ksqldb-server:8088
docker-compose up -d
docker-compose down
docker ps
docker volume rm kafka-wsl_kafka-data
docker logs kafka-wsl_kafka_1
nano docker-compose.yml

```
