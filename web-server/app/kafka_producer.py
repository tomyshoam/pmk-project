import json
from confluent_kafka import Producer
from .config import KAFKA_BOOTSTRAP_SERVERS


def create_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # optional: makes delivery a bit safer
        "enable.idempotence": True,
    }
    return Producer(conf)


def _delivery_report(err, msg):
    if err is not None:
        print(f"[Producer] Delivery failed: {err}")
    else:
        print(f"[Producer] Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def send_purchase_created(producer: Producer, topic: str, event: dict) -> None:
    payload = json.dumps(event).encode("utf-8")
    key = event["userId"].encode("utf-8")

    producer.produce(
        topic=topic,
        key=key,
        value=payload,
        callback=_delivery_report,
    )

    # ensures it actually goes out now (simple for learning)
    producer.flush(5)
