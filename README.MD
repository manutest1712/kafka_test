docker exec -it kafka-wsl_kafka_1   kafka-topics --create --topic test-topic --bootstrap-server localhost:9092
docker exec -it kafka-wsl_ksqldb-cli_1 ksql http://ksqldb-server:8088
docker-compose up -d
docker-compose down
docker ps
docker volume rm kafka-wsl_kafka-data
docker logs kafka-wsl_kafka_1
nano docker-compose.yml


version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 2181:2181
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-log:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - 9092:9092
    depends_on:
      - zookeeper
    volumes:
      - kafka-data:/var/lib/kafka/data
  ksqldb-server:
    image: confluentinc/cp-ksqldb-server:7.5.0
    depends_on:
      - kafka
    ports:
      - 8088:8088
    environment:
      KSQL_CONFIG_DIR: "/etc/ksqldb"
      KSQL_KSQL_SERVICE_ID: "ksql-service"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_BOOTSTRAP_SERVERS: "kafka:9092"
      KSQL_LISTENERS: "http://0.0.0.0:8088"

  ksqldb-cli:
    image: confluentinc/cp-ksqldb-cli:7.5.0
    depends_on:
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true

volumes:
  kafka-data:
  zookeeper-data:
  zookeeper-log:
