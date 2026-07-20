# Data Provider (Kafka Producer)

**Part of the BD26_Project_T1_B Real-Time Taxi Fleet Monitor** [⬅ Back to Main Project README](../README.md)

## Overview
The `data-provider` acts as the ingestion point of our pipeline. It is a Python-based Kafka Producer designed to simulate a real-time data stream by reading static `.txt` files from the T-Drive Trajectory Dataset and pushing them into our messaging system.

## Key Responsibilities

* **Data Ingestion:** Reads over 10,000 text files containing historical GPS records (Taxi ID, Timestamp, Longitude, Latitude).
* **Stream Simulation:** Replays the historical data into the `taxi-data` Kafka topic chronologically.
* **Speed Control:** Supports a configurable `SPEED_FACTOR` (managed via Docker Compose) to accelerate the replay speed of the historical dataset, allowing for high-throughput stress testing of the downstream Flink cluster.

## Data Structure requirement
For this module to work, you must place the T-Drive `.txt` files directly into the `data-provider/data/` directory.

For full installation and dataset download instructions, please refer to the [Main README](../README.md).