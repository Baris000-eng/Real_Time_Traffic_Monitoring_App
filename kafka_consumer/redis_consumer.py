from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import os
import time
from redis_writer import create_redis_client, write_to_redis, delete_from_redis

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "processed-taxi-data")


# kafka connection (with retry)
def connect_kafka():
    """This function establishes and returns a Kafka consumer connection.

    It retries connecting to Kafka until the broker becomes available.
    """
    # print("Trying to connect to Kafka...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_SERVER],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="redis-consumer",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            print("Connected to Kafka")
            return consumer

        except NoBrokersAvailable:
            print("Kafka not available...")
            time.sleep(5)


def process_stream(consumer, redis_client):
    """This function processes Kafka messages and writes them to Redis.

    It consumes streaming data from Kafka, extracts each record, and
    forwards it to Redis for storage. Each message is expected to be a
    deserialized JSON object (dictionary) representing a processed taxi
    event.
    
    Parameters
    ----------
    consumer : KafkaConsumer
        The Kafka consumer instance.
    redis_client : redis.Redis
        The Redis client instance.
    """
    print("Starting Kafka → Redis consumer...")

    for message in consumer:
        try:
            data = message.value  # already deserialized JSON (dict)
            taxi_id = data.get("taxiId")

            if data.get("status") == "END":
                delete_from_redis(redis_client, taxi_id)
                print(f"Deleted taxi:{taxi_id}")
                continue

            t1 = data.get("ingestion_time")
            if t1 is not None:
                t2 = time.time()
                delay_ms = round((t2 - float(t1)) * 1000, 2)
                redis_client.rpush("delay_log", delay_ms)
                redis_client.ltrim("delay_log", -20000, -1)

            write_to_redis(redis_client, data)

            print(f"Updated taxi:{taxi_id}")

        except Exception as e:
            print(f"Processing error: {e}")


def main():
    consumer = connect_kafka()
    redis_client = create_redis_client()
    print("Connected to Redis")

    process_stream(consumer, redis_client)


if __name__ == "__main__":
    main()
