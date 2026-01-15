import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.31.0.202:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "purchases.v1")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "purchase-consumer")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://3.75.132.150:27017")
MONGO_DB = os.getenv("MONGO_DB", "purchases_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "purchases")
