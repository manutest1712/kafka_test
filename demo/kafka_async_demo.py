import asyncio
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "my-first-python-topic-man"


# ------------------------------
# Delivery Callback Function
# ------------------------------
def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    """
    
    print ('In delivery report')
    if err is not None:
        print(f'[Producer] Message delivery failed: {err}')
    else:
        print(f'[Producer] Message successfully delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        
        
# ------------------------------
# 1. Admin: Create Topic
# ------------------------------
def create_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    topic = NewTopic(
        TOPIC,
        num_partitions=1,
        replication_factor=1
    )

    print(f"Creating topic: {TOPIC}")
    fs = admin.create_topics([topic])

    for t, f in fs.items():
        try:
            f.result()  
            print(f"Topic '{t}' created.")
        except Exception as e:
            if "TopicAlreadyExistsError" in str(e):
                print(f"Topic '{t}' already exists.")
            else:
                raise

# ------------------------------
# 2. Producer (async)
# ------------------------------
async def run_producer():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Sleep 5 seconds before starting sends
    print("Producer created, about to sleep")
    #await asyncio.sleep(5)
    print("Sleep returned — event loop is working")

    for i in range(5):
        msg = f"Message {i}"
        print(f"[Producer] Sending: {msg}")
        producer.produce(TOPIC, value=msg.encode("utf-8"), callback=delivery_report)
        print("Poll invoked")
        producer.poll(0)

    producer.flush()
    print("[Producer] Finished sending messages.")

# ------------------------------
# 3. Consumer (async)
# ------------------------------
async def run_consumer():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "my-first-consumer-group",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC])

    print("[Consumer] Waiting for messages...")

    received = 0
    target = 5

    while received < target:
        msg = consumer.poll(5.0)
        if msg is None:
            print("[Consumer] No message yet...")
            continue

        if msg.error():
            raise KafkaException(msg.error())

        print(f"[Consumer] Received: {msg.value().decode()}")
        received += 1

    consumer.close()
    print("[Consumer] Finished consuming messages.")

# ------------------------------
# 4. Delete Topic
# ------------------------------
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

# ------------------------------
# 5. Main Orchestrator
# ------------------------------
async def main():
    create_topic()

    t1 = asyncio.create_task(run_producer())
    t2 = asyncio.create_task(run_consumer())
    
    await t1
    await t2

    delete_topic()

# ------------------------------
# Run program
# ------------------------------
if __name__ == "__main__":
    asyncio.run(main())
