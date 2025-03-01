from utils.config_loader import load_config
from utils.kafka_utils import create_kafka_consumer
from utils.db_utils import MongoDB

def save_to_mongodb():
    config = load_config()
    consumer = create_kafka_consumer(config, config["kafka"]["target_topic"])
    mongo = MongoDB(config["mongodb"]["uri"], config["mongodb"]["database"], config["mongodb"]["collection"])

    buffer = []
    BATCH_SIZE = 10

    print(f"Reading from Kafka topic: {config['kafka']['target_topic']}")

    for message in consumer:
        try:
            buffer.append(message.value)

            if len(buffer) >= BATCH_SIZE:
                mongo.insert_bulk(buffer)
                buffer.clear()

        except Exception as e:
            print(f"Error processing message: {e}")

    # Insert remaining records if any
    if buffer:
        mongo.insert_bulk(buffer)
