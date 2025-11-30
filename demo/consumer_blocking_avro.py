from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.avro import AvroConsumer
import time
import datetime

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"     # REQUIRED for Avro decoding
TOPIC = "my-first-python-topic-man-avro"          # Must match your Avro producer


def delete_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(f"Deleting topic: {TOPIC}")

    fs = admin.delete_topics([TOPIC], operation_timeout=30)

    for t, f in fs.items():
        try:
            f.result()
            print(f"Topic '{t}' deleted.")
        except Exception as e:
            print(f"Could not delete topic '{t}': {e}")


def run_consumer():
    consumer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "consumer-group-avro",
        "schema.registry.url": SCHEMA_REGISTRY_URL,   # Important for Avro deserialization
        "auto.offset.reset": "earliest",
        # "specific.avro.reader": True,                # Only needed if using generated classes
    }
    
    
    consumer = AvroConsumer(consumer_config)
    
    consumer.subscribe([TOPIC])

    print("[Consumer] Waiting for Avro messages...")

    received = 0
    target = 10

    while received < target:
        msg = consumer.poll(5.0)

        if msg is None:
            print("[Consumer] No message yet...")
            continue

        if msg.error():
            raise KafkaException(msg.error())
        
         # Auto-deserialized key + value (Python dicts)
        key = msg.key()
        value = msg.value()

        print("------------------------------------------------")
        print(f"[Consumer] Received message #{received + 1} -- time - {datetime.datetime.now()}")
        print(f"Time      : {datetime.datetime.now()}")
        print(f"Key       : {key}")
        print(f"Value     : {value}")
        print("------------------------------------------------")
        
        
        received += 1

    consumer.close()
    print("[Consumer] Finished consuming all messages.")


if __name__ == "__main__":
    print("Consumer starting at", datetime.datetime.now())
    run_consumer()
    time.sleep(6)
    #delete_topic()
