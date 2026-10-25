import time

from confluent_kafka import Consumer, KafkaException, Producer


def create_consumer(connection, group_id, client_id):
    return Consumer({
        **connection,
        "group.id": group_id,
        "client.id": client_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
        "max.poll.interval.ms": 300000,
    })


def create_producer(connection):
    return Producer({
        **connection,
        "client.id": "product-view-bridge-producer",
        "acks": "all",
        "enable.idempotence": True,
        "delivery.timeout.ms": 30000,
    })


def poll_record(consumer):
    message = consumer.poll(timeout=1.0)

    if message is None:
        return None

    if message.error():
        raise KafkaException(message.error())

    return message


def publish_confirmed(producer, topic, key, value):
    """Publish one record and wait for delivery confirmation."""
    result = {}

    def on_delivery(error, message):
        result["error"] = error
        result["message"] = message

    deadline = time.monotonic() + 45

    while True:
        try:
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=on_delivery,
            )
            break
        except BufferError:
            if time.monotonic() >= deadline:
                raise TimeoutError("Producer queue is still full")

            producer.poll(0.2)

    while "error" not in result:
        if time.monotonic() >= deadline:
            raise TimeoutError("Timed out waiting for Kafka delivery confirmation")

        producer.poll(0.2)

    if result["error"] is not None:
        raise KafkaException(result["error"])

    return result["message"]