import base64
import json
import logging

from utils.db_utils import MongoDB
from utils.kafka_utils import create_consumer, poll_record


logger = logging.getLogger(__name__)


def decode_payload(encoded):
    if encoded is None:
        return None

    raw = base64.b64decode(encoded, validate=True)

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {
            "format": "base64",
            "data": encoded,
        }

    try:
        data = json.loads(text)
    except ValueError:
        return {
            "format": "text",
            "data": text,
        }

    return {
        "format": "json",
        "data": data,
    }


def validate_envelope(envelope):
    if not isinstance(envelope, dict):
        raise ValueError("Envelope must be an object")

    if envelope.get("version") != 1:
        raise ValueError("Unsupported envelope version")

    source = envelope.get("source")

    if not isinstance(source, dict):
        raise ValueError("Missing or invalid source metadata")

    for field in ("id", "topic"):
        if not isinstance(source.get(field), str) or not source[field]:
            raise ValueError(f"Invalid source.{field}")

    for field in ("partition", "offset"):
        value = source.get(field)

        if type(value) is not int or value < 0:
            raise ValueError(f"Invalid source.{field}")

    for field in ("key_b64", "value_b64"):
        value = envelope[field]

        if value is not None:
            if not isinstance(value, str):
                raise ValueError(f"Invalid {field}")

            base64.b64decode(value, validate=True)


def save_to_mongodb(config, stop_event):
    consumer = create_consumer(
        config["local"],
        config["local_group"],
        "product-view-mongo-consumer",
    )

    mongo = None

    try:
        mongo = MongoDB(
            config["mongo_uri"],
            config["mongo_database"],
            config["mongo_collection"],
        )

        mongo.ping()
        consumer.subscribe([config["local_topic"]])

        logger.info(
            "Sink started: topic=%s group=%s collection=%s.%s",
            config["local_topic"],
            config["local_group"],
            config["mongo_database"],
            config["mongo_collection"],
        )

        while not stop_event.is_set():
            message = poll_record(consumer)

            if message is None:
                continue

            if message.value() is None:
                raise ValueError(
                    "Local record is a tombstone, not an envelope"
                )

            envelope = json.loads(
                message.value().decode("utf-8")
            )

            validate_envelope(envelope)

            payload = decode_payload(envelope["value_b64"])

            document_id = mongo.upsert_event(
                envelope,
                payload,
            )

            # Commit the local offset only after the MongoDB write succeeds.
            consumer.commit(
                message=message,
                asynchronous=False,
            )

            logger.info(
                "Saved and committed: local=%s[%s]@%s mongo_id=%s",
                message.topic(),
                message.partition(),
                message.offset(),
                document_id,
            )

    finally:
        try:
            consumer.close()
        finally:
            if mongo is not None:
                mongo.close()