# Kafka Server (Remote)
KAFKA_BROKER = "113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294"
KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_USERNAME = "kafka"  
KAFKA_PASSWORD = "UnigapKafka@2024"  

# Kafka Topics
SOURCE_TOPIC = "product_view"          
TARGET_TOPIC = "processed_product_view" 

# MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "kafka_mongo_db"
COLLECTION_NAME = "product_data"
