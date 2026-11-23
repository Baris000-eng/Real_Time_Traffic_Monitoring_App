import json
from datetime import datetime
from average_speed_processor import AverageSpeedProcessor
from distance_processor import DistanceProcessor
from kafka_sink import create_kafka_sink
from location_filter import LocationFilter5s
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from taxi_processor import TaxiProcessor
from pyflink.common.time import Duration

from taxi_timestamp_assigner import TaxiTimestampAssigner

# Build the watermark strategy using the TaxiTimestampAssigner class. This strategy allows for a maximum latency of 5 seconds,
# which means that events can arrive up to 5 seconds late and still be processed in order. This is useful for taxi location 
# updates, which may not always arrive in the exact order they were generated, because of network delays. 
watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
    .with_timestamp_assigner(TaxiTimestampAssigner())


def run_beijing_taxi_pipeline():
    # Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)

    # docker compose down
    # docker compose up -d --build
    # docker compose logs -f flink-processor

    # Kafka Configuration
    kafka_props = {
        "bootstrap.servers": "kafka:29092",
        "group.id": "flink-processor-group",
        "auto.offset.reset": "earliest",
    }

    # Source from Kafka
    consumer = FlinkKafkaConsumer("taxi-data", SimpleStringSchema(), kafka_props)

    # Define Pipeline
    print("--- Submitting Beijing Taxi Job to Flink ---")

    # the keying is lost after each process, so we need to key by taxiId again before each process
    env.add_source(consumer).assign_timestamps_and_watermarks(watermark_strategy).name("Data Source").key_by(
        lambda x: json.loads(x)["taxiId"]
    ).process(TaxiProcessor(), output_type=Types.STRING()).name(
        "Taxi Processor"
    ).key_by(
        lambda x: json.loads(x)["taxiId"]
    ).process(
        DistanceProcessor(), output_type=Types.STRING()
    ).name(
        "Distance Processor"
    ).key_by(
        lambda x: json.loads(x)["taxiId"]
    ).process(
        AverageSpeedProcessor(), output_type=Types.STRING()
    ).name(
        "Average Speed Processor"
    ).key_by(
        lambda x: json.loads(x)["taxiId"]
    ).process(
        LocationFilter5s(), output_type=Types.STRING()
    ).name(
        "Location Filter (5s)"
    ).add_sink(
        create_kafka_sink()
    ).name(
        "Kafka sink"
    )

    #  Execute
    env.execute("Beijing Taxi Monitoring System")


if __name__ == "__main__":
    run_beijing_taxi_pipeline()
