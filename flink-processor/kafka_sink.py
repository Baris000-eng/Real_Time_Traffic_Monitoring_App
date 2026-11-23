from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
import json


def create_kafka_sink():
    """This function creates a Kafka sink for processed taxi events.

    The sink publishes processed stream data back into Kafka under the
    topic 'processed-taxi-data'. Each record is serialized as a JSON string.

    Example message:
    Key: null
    Value: JSON string like:
    {
    "taxiId": "12345",
    "timestamp": "2024-06-01T12:00:00Z",
    "latitude": 39.9042,
    "longitude": 116.4074,
    "avg_speed": 45.5,
    "total_distance": 120.3,
    "speedingViolation": false,
    "geofenceViolation": true
    }
    """
    return FlinkKafkaProducer(
        topic="processed-taxi-data",
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": "kafka:29092"},
    )
