"""
Kafka consumer service.
Consumes PurchaseCreated events and writes them to MongoDB.
"""

import json
from confluent_kafka import Consumer
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
)
from mongo import get_collection, insert_purchase


def create_consumer():
    """
    Create and return a Kafka consumer.
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)


def main():
    print("[Consumer] Starting Kafka consumer")

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    collection = get_collection()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[Consumer] Error: {msg.error()}")
                continue

            # Decode message
            event = json.loads(msg.value().decode("utf-8"))

            print(f"[Consumer] Received event: {event}")

            # Prepare Mongo document
            purchase_doc = {
                "_id": event["eventId"],     # idempotency key
                "eventVersion": event["eventVersion"],
                "eventType": event["eventType"],
                "timestamp": event["timestamp"],
                "userId": event["userId"],
                "itemId": event["itemId"],
                "quantity": event["qty"],
            }

            insert_purchase(collection, purchase_doc)

            # Commit offset AFTER successful processing
            consumer.commit(msg)

    except KeyboardInterrupt:
        print("[Consumer] Shutting down")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
