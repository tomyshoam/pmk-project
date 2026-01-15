from pymongo import MongoClient, errors, ASCENDING, DESCENDING
from .config import MONGO_URI, MONGO_DB, MONGO_COLLECTION


def get_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # helpful index for "get purchases by user ordered by time"
    collection.create_index([("userId", ASCENDING), ("timestamp", DESCENDING)])
    return collection


def insert_purchase(collection, purchase):
    try:
        collection.insert_one(purchase)
        print(f"[Mongo] Inserted purchase {purchase['_id']}")
        return True
    except errors.DuplicateKeyError:
        print(f"[Mongo] Duplicate event ignored: {purchase['_id']}")
        return True


def get_purchases_by_user(collection, user_id: str):
    cursor = collection.find({"userId": user_id}).sort("timestamp", -1)
    return list(cursor)
