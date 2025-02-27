from kafka import KafkaConsumer, KafkaProducer
import json
import config

def create_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",  # Kafka Local
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def produce_to_local_kafka():
    consumer = KafkaConsumer(
        config.SOURCE_TOPIC,
        bootstrap_servers=config.KAFKA_BROKER.split(","),
        security_protocol=config.KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=config.KAFKA_SASL_MECHANISM,
        sasl_plain_username=config.KAFKA_USERNAME,
        sasl_plain_password=config.KAFKA_PASSWORD,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    producer = create_producer()

    print(f"📡 Listening from remote Kafka topic: {config.SOURCE_TOPIC}")
    for message in consumer:
        data = message.value
        print(f"Producing to local Kafka topic: {data}")
        producer.send(config.TARGET_TOPIC, value=data)
