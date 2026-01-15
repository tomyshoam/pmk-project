import json
from confluent_kafka import Consumer
from pydantic import ValidationError

from .config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID
from .mongo import insert_purchase
from .models import PurchaseCreatedEvent


def create_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)


def run_consumer(collection, stop_flag) -> None:
    """
    Blocking loop that consumes Kafka and writes to Mongo.
    `stop_flag` is any object with `.is_set()` (threading.Event).
    """
    print("[Consumer] Starting Kafka consumer")

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while not stop_flag.is_set():
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[Consumer] Kafka error: {msg.error()}")
                continue

            try:
                raw = msg.value().decode("utf-8")
                data = json.loads(raw)
                event = PurchaseCreatedEvent.model_validate(data)  # enforces `quantity`
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                print(f"[Consumer] Bad payload (decode/json): {e}. Skipping. offset={msg.offset()}")
                # You can choose to commit or not; for now commit to avoid poison-pill loops:
                consumer.commit(msg)
                continue
            except ValidationError as e:
                print(f"[Consumer] Bad event schema: {e}. data={data}")
                consumer.commit(msg)
                continue

            print(f"[Consumer] Received event: {event.model_dump()} (p={msg.partition()} o={msg.offset()})")

            purchase_doc = {
                "_id": event.eventId,
                "eventVersion": event.eventVersion,
                "eventType": event.eventType,
                "timestamp": event.timestamp,
                "userId": event.userId,
                "itemId": event.itemId,
                "quantity": event.quantity,
            }

            ok = insert_purchase(collection, purchase_doc)
            if ok:
                consumer.commit(msg)

    except Exception as e:
        print(f"[Consumer] Fatal error in loop: {e}")
        raise
    finally:
        consumer.close()
        print("[Consumer] Closed")
