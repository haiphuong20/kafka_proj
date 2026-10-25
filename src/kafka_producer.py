import base64
import json
import logging

from utils.kafka_utils import (
    create_consumer,
    create_producer,
    poll_record,
    publish_confirmed,
)


logger = logging.getLogger(__name__)


def encode_bytes(value):
    if value is None:
        return None

    return base64.b64encode(value).decode("ascii")


def build_envelope(config, message):
    timestamp_type, timestamp_ms = message.timestamp()

    return {
        "version": 1,
        "source": {
            "id": config["source_id"],
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "timestamp_type": timestamp_type,
            "timestamp_ms": timestamp_ms,
        },
        "key_b64": encode_bytes(message.key()),
        "value_b64": encode_bytes(message.value()),
        "headers": [
            {
                "name": name,
                "value_b64": encode_bytes(value),
            }
            for name, value in (message.headers() or [])
        ],
    }


def produce_to_local_kafka(config, stop_event):
    consumer = create_consumer(
        config["source"],
        config["source_group"],
        "product-view-source-consumer",
    )

    producer = None

    try:
        producer = create_producer(config["local"])
        consumer.subscribe([config["source_topic"]])

        logger.info(
            "Bridge started: source=%s target=%s group=%s",
            config["source_topic"],
            config["local_topic"],
            config["source_group"],
        )

        while not stop_event.is_set():
            message = poll_record(consumer)

            if message is None:
                continue

            envelope = build_envelope(config, message)

            delivered = publish_confirmed(
                producer=producer,
                topic=config["local_topic"],
                key=message.key(),
                value=json.dumps(
                    envelope,
                    ensure_ascii=False,
                ).encode("utf-8"),
            )

            # Commit the source offset only after successful delivery to local Kafka.
            consumer.commit(
                message=message,
                asynchronous=False,
            )

            logger.info(
                "Delivered and committed: "
                "source=%s[%s]@%s local=%s[%s]@%s",
                message.topic(),
                message.partition(),
                message.offset(),
                delivered.topic(),
                delivered.partition(),
                delivered.offset(),
            )

    finally:
        try:
            if producer is not None:
                remaining = producer.flush(timeout=35)

                if remaining:
                    logger.error(
                        "Producer still has %s undelivered records",
                        remaining,
                    )
        finally:
            consumer.close()