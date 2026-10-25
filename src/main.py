import argparse
import logging
import signal
from threading import Event

from kafka_consumer import save_to_mongodb
from kafka_producer import produce_to_local_kafka
from utils.config_loader import load_config


def main():
    parser = argparse.ArgumentParser(
        description="Source Kafka -> Local Kafka -> MongoDB"
    )

    parser.add_argument(
        "mode",
        choices=["bridge", "sink"],
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    stop_event = Event()

    def request_stop(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        config = load_config()

        if args.mode == "bridge":
            produce_to_local_kafka(config, stop_event)
        else:
            save_to_mongodb(config, stop_event)

    except Exception:
        logging.exception("Pipeline stopped because of an error")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())