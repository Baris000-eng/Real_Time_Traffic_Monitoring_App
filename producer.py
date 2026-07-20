import json
import os
import time

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "taxi-data")
DATA_PATH = os.getenv("DATA_DIR", "./data")
SIMULATION_SPEED = float(os.getenv("SPEED_FACTOR", "5.0"))
# FLOOD_MODE disables replay sleeps and sends data as fast as Kafka accepts it.
# Use it for throughput/latency benchmarking, not for normal dashboard demos.
FLOOD_MODE = os.getenv("FLOOD_MODE", "false").lower() == "true"


def connect_to_kafka(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            print(
                f"Connecting to Kafka at {KAFKA_SERVER} (attempt {attempt}/{retries})..."
            )
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        
                key_serializer=lambda k: str(k).encode("utf-8"),
              
                request_timeout_ms=30000,
              
                retries=3,
            )
            print("Connected to Kafka successfully.")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready yet. Waiting {delay}s before retry...")
            time.sleep(delay)

    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts.")

def load_all_data(data_path):
    if not os.path.exists(data_path):
        raise FileNotFoundError(
            f"Data directory '{data_path}' not found. "
            f"Make sure your .txt files are in data-provider/data/"
        )

    all_files = [f for f in os.listdir(data_path) if f.endswith(".txt")]

    if not all_files:
        raise FileNotFoundError(
            f"No .txt files found in '{data_path}'. "
            f"Copy the T-Drive .txt files into data-provider/data/"
        )

    print(f"Found {len(all_files)} taxi files. Loading...")

    data_frames = []
    for filename in all_files:
        filepath = os.path.join(data_path, filename)
        try:
            df = pd.read_csv(
                filepath,
                names=["taxiId", "timestamp", "lon", "lat"],
                on_bad_lines="skip",
            )
            df.dropna(subset=["taxiId", "timestamp", "lon", "lat"], inplace=True)
            data_frames.append(df)
        except Exception as e:
            print(f"Warning: could not read {filename} — {e}. Skipping.")
            continue

    if not data_frames:
        raise ValueError("All data files were empty or unreadable.")

    full_data = pd.concat(data_frames, ignore_index=True)
    full_data["timestamp"] = pd.to_datetime(full_data["timestamp"])
    print("The old length: "+str(len(full_data))+"")
    full_data = full_data.drop_duplicates()
    full_data = full_data.sort_values("timestamp")
    

    # For two taxis appearing in the exact same location at the same time (ghost taxis)
    full_data = full_data.drop_duplicates(subset=["timestamp", "lat", "lon"], keep='first')

    # For the same taxi appearing in different places at the same time 
    full_data = full_data.drop_duplicates(subset=["taxiId", "timestamp"], keep='first')
    print("The new length: "+str(len(full_data))+"")

    print(f"Loaded {len(full_data)} total GPS events across {len(all_files)} taxis.")
    return full_data

def replay(producer, full_data):
    print(f"Starting stream at {SIMULATION_SPEED}x submission speed...")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Flood mode: {FLOOD_MODE}")
    print("-" * 50)

    total_sent = 0
    batch_count = 0

    last_timestamps = full_data.groupby("taxiId")["timestamp"].max()
    prev_timestamp = None

    try:
        for timestamp, group in full_data.groupby("timestamp"):

            if not FLOOD_MODE:
                if prev_timestamp is not None:
                    time_diff_seconds = (timestamp - prev_timestamp).total_seconds()
                    sleep_duration = time_diff_seconds / SIMULATION_SPEED
                    if sleep_duration > 0:
                        time.sleep(sleep_duration)
                else:
                    time.sleep(1.0 / SIMULATION_SPEED)

            prev_timestamp = timestamp

            for _, row in group.iterrows():
                producer.send(
                    TOPIC_NAME,
                    key=str(int(row["taxiId"])),
                    value={
                        "taxiId":    int(row["taxiId"]),
                        "timestamp": str(row["timestamp"]),
                        "lon": float(row["lon"]),
                        "lat": float(row["lat"]),
                        # T1 timestamp for end-to-end latency measurement.
                        # Flink/Redis can compare this with its processing timestamp.
                        "ingestion_time": time.time()
                    }
                )
                total_sent += 1

            producer.flush()
            batch_count += 1

            # Change 10000 to 20, so we actually see logs streaming every 20 seconds of data!
            if batch_count % 20 == 0:
                print(f" Streaming Data -> Timestamp: {timestamp} | Total events sent: {total_sent}")

            for taxi_id in group["taxiId"].unique():
                if last_timestamps[taxi_id] == timestamp:
                    producer.send(
                        TOPIC_NAME,
                        key=str(int(taxi_id)),
                        value={
                            "taxiId": int(taxi_id),
                            "status": "END",
                            "timestamp": str(timestamp),
                        },
                    )
                    print(f"END token sent for taxi {int(taxi_id)}")

            producer.flush()
            time.sleep(1.0 / SIMULATION_SPEED)

        print("-" * 50)
        print(f"Stream complete. Sent {total_sent} events in {batch_count} batches.")

    except KeyboardInterrupt:
        print("\nStream interrupted by user.")

    except Exception as e:
        print(f"Error during stream: {e}")
        raise

if __name__ == "__main__":
    print("=" * 50)
    print("Taxi Fleet — Kafka Data Provider")
    print(f"Kafka:           {KAFKA_SERVER}")
    print(f"Topic:           {TOPIC_NAME}")
    print(f"Data directory:  {DATA_PATH}")
    print(f"Simulation speed: {SIMULATION_SPEED}x")
    print("=" * 50)

    # load data first (because it can take a while for full dataset)
    full_data = load_all_data(DATA_PATH)

    # only after data is fully loaded, then connect to Kafka so connection doesn't time out
    producer = connect_to_kafka()
    replay(producer, full_data)
