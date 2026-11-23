# Kafka Consumers (Storage Sinks)

**Part of the BD26_Project_T1_B Real-Time Taxi Fleet Monitor** [⬅ Back to Main Project README](../README.md)

## Overview
The `kafka_consumer` directory contains the Python scripts responsible for reading the enriched and filtered data from the `processed-taxi-data` Kafka topic and persisting it into our dual-storage architecture.

## Consumer Scripts

### 1. Redis Consumer (`redis_consumer.py`)
* **Purpose:** Maintains the real-time state of the taxi fleet.
* **Function:** Constantly overwrites the latest location, speed, and violation status of each taxi in a Redis in-memory cache.
* **Benefit:** Enables sub-millisecond query responses for the Flask Web Dashboard, ensuring a lag-free live map visualization.

### 2. PostgreSQL Consumer (`postgres_consumer.py`)
* **Purpose:** Handles long-term archival and historical data retention.
* **Function:** Appends every processed event into the PostgreSQL `taxi_events` table using JSONB storage.
* **Benefit:** Allows for complex historical batch analysis, aggregations, and auditing of taxi trajectories over time.

For database port configurations or service management, please refer to the [Main README](../README.md).