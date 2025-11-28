from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time
import datetime

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "my-first-python-topic-man"


def delivery_report(err, msg):
    if err is not None:
        print(f"[Producer] Delivery failed: {err}")
    else:
        print(f"[Producer] Delivered: {msg.value().decode()} "
              f"to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} -- time {datetime.datetime.now()}")


def create_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # -------------------------------
    # Check if topic already exists
    # -------------------------------
    metadata = admin.list_topics(timeout=10)

    if TOPIC in metadata.topics:
        print(f"Topic '{TOPIC}' already exists. Skipping creation.")
        return
    
    topic = NewTopic(TOPIC, num_partitions=5, replication_factor=1,
                    config={
        "cleanup.policy": "compact",
        "compression.type": "snappy"
    })

    print(f"Creating topic: {TOPIC}")
    fs = admin.create_topics([topic])

    for t, f in fs.items():
        try:
            f.result()
            print(f"Topic '{t}' created.")
        except Exception as e:
            if "TopicAlreadyExists" in str(e):
                print(f"Topic '{t}' already exists.")
            else:
                raise


def run_producer():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print("Producer created. Sleeping 5 seconds...")
    time.sleep(5)
    print("Producer starting to send messages")

    for i in range(50):
        key = f"key-{i % 10}"   # repeating keys to test compaction
        msg = f"Message {i}"
        print(f"[Producer] Sending: {msg}")
        producer.produce(TOPIC, key=key.encode(), value=msg.encode(), callback=delivery_report)
        producer.poll(0)  # Trigger callback
        time.sleep(0.1)

    producer.flush()
    print("[Producer] Finished sending messages.")


if __name__ == "__main__":
    print("Producer starting at", datetime.datetime.now())
    create_topic()
    run_producer()
