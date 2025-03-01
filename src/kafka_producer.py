from utils.config_loader import load_config
from utils.kafka_utils import create_kafka_producer, create_kafka_consumer

def produce_to_local_kafka():
    config = load_config()
    producer = create_kafka_producer(config)
    consumer = create_kafka_consumer(config, config["kafka"]["source_topic"])

    print(f"📡 Listening to Kafka topic: {config['kafka']['source_topic']}")

    for message in consumer:
        data = message.value
        print(f"Producing to local Kafka: {data}")

        def on_send_success(record_metadata):
            print(f"✅ Message sent to {record_metadata.topic} partition {record_metadata.partition}")

        def on_send_error(ex):
            print(f"Error sending message: {ex}")

        producer.send(
            config["kafka"]["target_topic"], value=data
        ).add_callback(on_send_success).add_errback(on_send_error)
