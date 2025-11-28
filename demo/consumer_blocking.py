from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
import time
import datetime

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "my-first-python-topic-man"


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
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "consumer-group-blocking",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC])

    print("[Consumer] Waiting for messages...")

    received = 0
    target = 10

    while received < target:
        msg = consumer.poll(5.0)

        if msg is None:
            print("[Consumer] No message yet...")
            continue

        if msg.error():
            raise KafkaException(msg.error())

        print(f"[Consumer] Received: {msg.value().decode()} -- time - {datetime.datetime.now()}")
        received += 1

    consumer.close()
    print("[Consumer] Finished consuming all messages.")


if __name__ == "__main__":
    print("Consumer starting at", datetime.datetime.now())
    run_consumer()
    time.sleep(6)
    delete_topic()
