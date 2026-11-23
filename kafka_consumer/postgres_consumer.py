import json
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from postgres_writer import (
    create_postgres_connection,
    ensure_postgres_schema,
    write_to_postgres,
)

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "processed-taxi-data")


def connect_kafka():
    """This function establishes and returns a Kafka consumer connection.

    It retries connecting to Kafka until the broker becomes available.
    """
    while True:
        try:
            return KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_SERVER],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="postgres-consumer",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print("Kafka not available...")
            time.sleep(5)


def main():
    """This function starts the PostgreSQL consumer pipeline."""
    consumer = connect_kafka()

    postgres_conn = create_postgres_connection()
    ensure_postgres_schema(postgres_conn)

    print("PostgreSQL consumer started")

    for message in consumer:
        write_to_postgres(postgres_conn, message.value)


if __name__ == "__main__":
    main()