import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.31.0.202:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "purchases.v1")

# API Server (internal VPC address)
API_SERVER_URL = os.getenv("API_SERVER_URL", "http://172.31.2.3:8000")
