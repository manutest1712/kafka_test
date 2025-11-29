from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time
import datetime

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"   # IMPORTANT
TOPIC = "my-first-python-topic-man-avro"

# ---------------------- LOAD SCHEMAS FROM FILE -----------------------
def load_schema(path):
    with open(path, "r") as f:
        return avro.loads(f.read())

def delivery_report(err, msg):
    if err is not None:
        print(f"[Producer] Delivery failed: {err}")
    else:
        print(f"[Producer] Delivered message "
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

    topic = NewTopic(
        TOPIC,
        num_partitions=5,
        replication_factor=1,
        config={
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
    )

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
    print("Initializing AvroProducer...")

    value_schema = load_schema("schemas/arrival_value.json")
    key_schema = load_schema("schemas/arrival_key.json")

    producer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "acks": "all"
    }

    producer = AvroProducer(
        config=producer_config,
        default_value_schema=value_schema
    )

    print("AvroProducer created. Sleeping 5 seconds...")
    time.sleep(5)
    print("Sending Avro messages...")

    for i in range(50):
        key = f"key-{i % 10}"   # repeating keys to test compaction

        station_id = 100 + (i % 5)
        train_id = 5000 + (i %5)
        arrival_event = {
            "station_id": station_id,
            "train_id": train_id ,
            "direction": "N" if i % 2 == 0 else "S",
            "line": "green",
            "train_status": "on_time" if i % 3 else "delayed",
            "prev_station_id": None,
            "prev_direction": None
        }

        print(f"[Producer] Sending: {arrival_event}")
        producer.produce(topic=TOPIC, key={"station_id": station_id, "train_id": train_id},
                         value=arrival_event,
                         callback=delivery_report, key_schema=key_schema
                         )

        producer.poll(0)  # Trigger callback
        time.sleep(0.1)

    print("Flushing...")
    producer.flush()
    print("Done sending Avro messages.")


if __name__ == "__main__":
    print("Producer starting at", datetime.datetime.now())
    create_topic()
    run_producer()
