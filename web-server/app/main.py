from fastapi import FastAPI, HTTPException
from uuid import uuid4
from datetime import datetime, timezone

from .models import BuyRequest, PurchaseCreatedEvent
from .config import KAFKA_TOPIC
from .kafka_producer import create_producer, send_purchase_created
from .api_client import get_all_bought_items

app = FastAPI(title="Client Web Server")

producer = None


@app.on_event("startup")
def on_startup():
    global producer
    producer = create_producer()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/buy")
def buy(req: BuyRequest):
    # Build event
    event = PurchaseCreatedEvent(
        eventId=str(uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        userId=req.userId,
        itemId=req.itemId,
        quantity=req.quantity,
    ).model_dump()

    try:
        send_purchase_created(producer, KAFKA_TOPIC, event)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to produce event: {e}")

    # async semantics: accepted for processing
    return {"status": "accepted", "eventId": event["eventId"]}


@app.get("/getAllBoughtItems")
def get_all(userId: str):
    if not userId:
        raise HTTPException(status_code=400, detail="userId is required")

    try:
        return get_all_bought_items(userId)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"API server error: {e}")
