# Kafka Server (Remote)
KAFKA_BROKER = "113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294"
KAFKA_SECURITY_PROTOCOL = "SASL_PLAINTEXT"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_USERNAME = ""  # Thay bằng username của bạn
KAFKA_PASSWORD = ""  # Thay bằng password của bạn

# Kafka Topics
SOURCE_TOPIC = "product_view"          # Topic nguồn (Kafka Server)
TARGET_TOPIC = "processed_product_view" # Topic đích (Kafka Local)

# MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "kafka_mongo_db"
COLLECTION_NAME = "product_data"
