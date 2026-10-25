import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def required(name):
    value = os.getenv(name)

    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def kafka_connection(prefix):
    config = {
        "bootstrap.servers": required(
            f"{prefix}_BOOTSTRAP_SERVERS"
        ),
        "security.protocol": os.getenv(
            f"{prefix}_SECURITY_PROTOCOL",
            "SASL_PLAINTEXT",
        ),
    }

    if config["security.protocol"].startswith("SASL"):
        config.update({
            "sasl.mechanism": os.getenv(
                f"{prefix}_SASL_MECHANISM",
                "PLAIN",
            ),
            "sasl.username": required(f"{prefix}_USERNAME"),
            "sasl.password": required(f"{prefix}_PASSWORD"),
        })

    return config


def load_config():
    load_dotenv(PROJECT_ROOT / ".env")

    return {
        "source": kafka_connection("SOURCE"),
        "local": kafka_connection("LOCAL"),
        "source_topic": required("SOURCE_TOPIC"),
        "local_topic": required("LOCAL_TOPIC"),
        "source_group": required("SOURCE_GROUP_ID"),
        "local_group": required("LOCAL_GROUP_ID"),
        "source_id": required("SOURCE_ID"),
        "mongo_uri": required("MONGO_URI"),
        "mongo_database": required("MONGO_DATABASE"),
        "mongo_collection": required("MONGO_COLLECTION"),
    }