from kafka import KafkaProducer, KafkaConsumer
import json

def create_kafka_producer(config):
    return KafkaProducer(
        bootstrap_servers=config["kafka"]["bootstrap_servers"].split(","),
        security_protocol=config["kafka"]["security_protocol"],
        sasl_mechanism=config["kafka"]["sasl_mechanism"],
        sasl_plain_username=config["kafka"]["username"],
        sasl_plain_password=config["kafka"]["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def create_kafka_consumer(config, topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=config["kafka"]["bootstrap_servers"].split(","),
        security_protocol=config["kafka"]["security_protocol"],
        sasl_mechanism=config["kafka"]["sasl_mechanism"],
        sasl_plain_username=config["kafka"]["username"],
        sasl_plain_password=config["kafka"]["password"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
