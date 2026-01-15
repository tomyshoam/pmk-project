from fastapi import FastAPI, HTTPException
from threading import Thread, Event

from .mongo import get_collection, get_purchases_by_user
from .kafka_consumer import run_consumer

app = FastAPI(title="API Server")

stop_event = Event()
consumer_thread: Thread | None = None

collection = None


@app.on_event("startup")
def on_startup():
    global consumer_thread, collection
    collection = get_collection()

    consumer_thread = Thread(target=run_consumer, args=(collection, stop_event), daemon=True)
    consumer_thread.start()


@app.on_event("shutdown")
def on_shutdown():
    stop_event.set()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/purchases")
def get_purchases(userId: str):
    if not userId:
        raise HTTPException(status_code=400, detail="userId is required")
    docs = get_purchases_by_user(collection, userId)

    # Convert Mongo Object/fields if needed; _id is already string
    return {"userId": userId, "purchases": docs}
