from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import config

def create_mongo_collection():
    client = MongoClient(config.MONGO_URI)
    return client[config.DATABASE_NAME][config.COLLECTION_NAME]

def save_to_mongodb():
    consumer = KafkaConsumer(
        config.TARGET_TOPIC,
        bootstrap_servers="localhost:9092",  # Kafka Local
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    collection = create_mongo_collection()

    print(f"Reading from local Kafka topic: {config.TARGET_TOPIC}")
    for message in consumer:
        data = message.value
        print(f"Saving to MongoDB: {data}")
        collection.insert_one(data)
