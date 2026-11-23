# Real-Time Taxi Fleet Monitor — BD26_Project_T1_B

**Module:** Big Data — Lab Exercises  
**Team:** BD26_Project_T1_B

## Description

This project implements a real-time taxi fleet monitoring system built on top of Apache Kafka, Apache Flink, Redis, and Flask. GPS location data from over 10,000 Beijing taxis is streamed through a processing pipeline that calculates speed, distance, and violations in real time, and visualises the results on a live web dashboard.

The system is based on the [T-Drive trajectory dataset](https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/) from Microsoft Research, which contains GPS recordings from 10,357 taxis driving around Beijing over one week.

## Public URL of Our Project: http://4.230.18.158/

## DockerHub URL of Our Project: https://hub.docker.com/u/bkaplan18

## Azure VM Deployment

The cloud deployment runs on an Azure Ubuntu VM using `docker-compose.azure.yml`.
This compose file pulls pre-built AMD64 DockerHub images and does not build images on the VM.

Public dashboard URL:

```text
http://4.230.18.158/
```

### Required VM File Structure

The Azure VM only needs the Azure compose file and the taxi dataset files:

```text
/home/azureuser/
├── docker-compose.azure.yml
└── data-provider/
    └── data/
        ├── 1.txt
        ├── 2.txt
        └── ...
```

### Start the Azure Deployment

```bash
docker compose -f docker-compose.azure.yml pull
docker compose -f docker-compose.azure.yml up -d
```

### Check Running Containers

```bash
docker compose -f docker-compose.azure.yml ps
```

### View Logs

```bash
docker compose -f docker-compose.azure.yml logs -f
```

### Stop the Azure Deployment

```bash
docker compose -f docker-compose.azure.yml down
```

The dashboard is exposed through public HTTP port `80`, mapped to the dashboard container port `5050`:

```yaml
ports:
  - "80:5050"
```

Therefore, the dashboard is accessible at:

```text
http://4.230.18.158/
```

## Team Members

- Alina Voilova
- Barış Kaplan
- Merve Sarısoy
- Saweira Rauf
- Trung Quan (Zach) Nguyen
- Yaren Karabacak

## System Architecture

The real-time taxi monitoring system follows a streaming data pipeline architecture:

```text
+-----------------------+
|    T-Drive Dataset    |
|     (GPS Records)     |
+-----------+-----------+
            |
            v
+-----------------------+
|     Data Provider     |
|   (Kafka Producer)    |
+-----------+-----------+
            |
            | Raw Taxi Data
            v
+-----------------------+
|      Kafka Topic      |
|      taxi-data        |
+-----------+-----------+
            |
            v
+-----------------------+
|    Flink Processor    |
|  Stream Processing    |
+-----------+-----------+
            |
            | Filtered & Processed
            v
+-----------------------+
|      Kafka Topic      |
| processed-taxi-data   |
+-----------+-----------+
            |
      +-----+-------------------------+
      |                               |
      v                               v
+-----------+                   +-------------+
|   Redis   |                   | PostgreSQL  |
+-----+-----+                   +------+------+
  |                                   |
  v                                   v
+-----------------------+   +-----------------------+
|   Flask Dashboard     |   | Historical Analysis   |
|       (Web UI)        |   | (PostgreSQL Queries)  |
+-----------------------+   +-----------------------+
```


### Pipeline Components and Processing Stages

**1. Data Provider (data-provider/)**
- Reads GPS records from T-Drive dataset files (1000+ .txt files)
- Acts as a Kafka producer, streaming taxi location data
- Sends raw taxi records containing: taxi ID, timestamp, latitude, longitude

**2. Kafka Input Topic (taxi-data)**
- Message broker that buffers incoming GPS records
- Enables decoupling between data production and processing
- Allows multiple consumers to independently process the same data stream

**3. Apache Flink Stream Processor (flink-processor/)**

The Flink processor performs multi-stage stream processing:

- **Stage 1 - Validation and Event Preparation (TaxiProcessor):**
  - Parses and validates JSON input, timestamps, and coordinates
  - Propagates `END` events downstream so consumers can clear state
  - Applies optional map-matching (snap-to-road) via STRtree when enabled
  - Rejects out-of-order records and records outside service radius (>15 km)
  - Flags area/speed violations on valid records

- **Stage 2 - Cumulative Distance (DistanceProcessor):**
  - Tracks previous position per taxi and computes point-to-point Haversine distance
  - Maintains `total_distance_km` per taxi in Flink state
  - Clears distance state on `END`

- **Stage 3 - Running Average Speed (AverageSpeedProcessor):**
  - Maintains cumulative speed sum/count per taxi
  - Adds `avg_speed` to each outgoing event
  - Clears speed state on `END`

- **Stage 4 - 5s Emission Throttling (LocationFilter5s):**
  - Emits at most one event per taxi every 5 seconds
  - Reduces downstream update volume for storage/UI consumers
  - Passes `END` events through immediately

**4. Kafka Output Topic (processed-taxi-data)**
- Contains enriched, filtered, and processed taxi events
- Ready for downstream consumption by storage and visualization systems

**5. Kafka Consumers (kafka_consumer/)**

- **Redis Consumer (redis_consumer.py):**
  - Writes processed events to Redis cache
  - Enables fast in-memory queries for dashboard
  - Stores latest taxi states for real-time visualization

- **PostgreSQL Consumer (postgres_consumer.py):**
  - Persists all processed events to PostgreSQL database
  - Creates `taxi_events` table with JSONB storage for event data
  - Enables historical analysis and long-term data retention

**6. Data Storage**

- **Redis:** High-speed cache for real-time dashboard queries (millisecond-level access)
- **PostgreSQL:** Persistent database for event history, analysis, and archival

**7. Flask Dashboard (dashboard/)**
- Web application serving real-time taxi fleet visualization
- Reads live taxi state and metrics from Redis (not PostgreSQL)
- Displays live map of Beijing with active taxi locations
- Shows speed, distance, and violation metrics
- Updates in real-time as new events arrive
- Accessible at `http://localhost:5050`

## System Requirements

### Hardware Requirements

| Component | Minimum | Recommended |
| --- | --- | --- |
| **CPU** | 4 cores | 8+ cores |
| **RAM** | 8 GB | 16 GB or more |
| **Disk Space** | 50 GB | 100 GB+ (for full T-Drive dataset) |
| **Network** | 100 Mbps | Gigabit Ethernet |

### Software Requirements

- **Operating System:** Windows 10/11, macOS (Intel/Apple Silicon), or Linux
- **Docker Desktop:** 4.0+ (or Docker Engine + Docker Compose 2.0+)
- **Git:** 2.30+
- **Browser:** Modern browser for dashboard (Chrome, Firefox, Safari, Edge)

### Port Requirements

The following ports must be available on your system:

| Service | Port | Purpose |
| --- | --- | --- |
| **Dashboard** | 5050 | Flask web UI |
| **Kafka** | 9092 | Kafka broker (external) |
| **Flink JobManager** | 8081 | Flink web UI |
| **PostgreSQL** | 5432 | Database access |
| **Redis** | 6379 | In-memory cache |
| **Zookeeper** | 2181 | Kafka coordination |

### Disk Space Requirements

- **Docker images:** ~3–4 GB (built locally)
- **T-Drive dataset (full):** ~28 GB (10,357 taxi files, ~1 week of data)
  - For development testing: Use 10–100 files (~30–300 MB)
- **Database storage:** ~2–5 GB per day of processed events (depending on taxi activity and filtering)
- **Total minimum:** ~50 GB recommended

### Network Requirements

- Stable internet connection for initial Docker image pulls
- Internal network communication between containers (automatic with Docker Compose)
- No external network calls required after initial setup

## Setup Instructions

### Requirements

- Docker Desktop — must be running before any docker command
- Git
- T-Drive dataset (download instructions in Setup step 2)

### Step 1 — Clone the Repository

```bash
git clone https://collaborating.tuhh.de/e-19/teaching/bd26_project_t1_b.git
cd bd26_project_t1_b
```

### Step 2 — Download and Place the Dataset

1. Download the T-Drive dataset from Microsoft Research:
   https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/

2. Open the `taxi_log_2008_by_id` folder

3. Copy only the `.txt` files directly into `data-provider/data/`:

```
data-provider/
└── data/
    ├── 1.txt
    ├── 2.txt
    └── ...
```

> Do not copy the entire folder itself, `userguide.pdf`, or `Thumbs.db`.
> For quick testing during development, copy only 10 files to start with.

### Step 3 — Start All Docker Containers

```bash
docker-compose up -d --build
```

Initial build may take a few minutes for setup. Subsequent runs are even faster thanks to Docker layer caching.

### Step 4 — Verify if All Docker Containers are Running

```bash
docker-compose ps
```

All services should show `Up`:

```
bd26_project_t1_b-dashboard-1             Up
bd26_project_t1_b-flink-jobmanager-1      Up
bd26_project_t1_b-flink-processor-1       Up
bd26_project_t1_b-flink-taskmanager-1     Up
bd26_project_t1_b-kafka-1                 Up
bd26_project_t1_b-postgres-1              Up
bd26_project_t1_b-postgres-consumer-1     Up
bd26_project_t1_b-producer-1              Up
bd26_project_t1_b-redis-1                 Up
bd26_project_t1_b-redis-consumer-1        Up
bd26_project_t1_b-zookeeper-1             Up
```

## Checking content of processed stream

```bash
docker exec -it bd26_project_t1_b-kafka-1 bash
kafka-topics --bootstrap-server kafka:29092 --list
kafka-console-consumer --bootstrap-server kafka:29092 --topic processed-taxi-data --from-beginning
```

### Step 5 - How to Stop and Delete All Running Containers?

In order to stop and delete all running containers, we can use the command below:
docker compose down

### Step 6 - How to Stop, Start, and Restart a Specific Container?

In order to stop the Docker container of the dashboard module/service, we can use the command below:
docker compose stop dashboard

In order to start the Docker container of the dashboard module/service, we can use the command below:
docker compose start dashboard

In order to restart the Docker container of the dashboard module/service, we can use the command below:
docker compose restart dashboard

Note: Depending on the specific module/service which you want to stop/start/restart the Docker container for, you may need to replace
the existing module/service name, with the one you desired. In the commands above, the dashboard service is used for example.

## Docker Containers Access Guide

To evaluate the project, you can pull the pre-built and public Docker images directly from DockerHub and manage the multi-container environment
using the provided `docker-compose.yml`.

### 1. Pulling the Public Docker Images

The images are hosted publicly on DockerHub and do not require any authentication (`docker login`) to pull. Run the following commands to download them:

```bash
docker pull bkaplan18/data-provider:latest
docker pull bkaplan18/flink-processor:latest
docker pull bkaplan18/dashboard:latest
docker pull bkaplan18/flink-jobmanager:latest
docker pull bkaplan18/flink-taskmanager:latest
docker pull bkaplan18/postgres-consumer:latest
docker pull bkaplan18/producer:latest
docker pull bkaplan18/redis-consumer:latest
```

### 2. Creating and Running Containers from the Downloaded Public Docker Images in the Background:

In order to create and run containers from the downloaded Docker images, we can use the command below:
docker compose up -d

In the 'docker compose up -d' command, if any changes to code or Dockerfiles are to be done, we need to pass 
'--build' argument in this command (docker compose up -d --build) to build the recent Docker image. Here, '-d' 
argument makes the Docker container work in the detatched mode, which runs the container in the background that 
allow us to use the terminal when the container services are running up. If we do not use the '-d' argument, then 
the Docker container will work in the attached mode, which runs the container in the foreground that will prevent 
us to use the terminal when the container services are running up. 

In order to create and run a specific Docker container from a downloadedDocker image, we can use the command below:
docker run downloaded_docker_image_name

If we want to specify a name to the Docker Container, we can pass '--name' argument to the command above:
docker run --name my_docker_container downloaded_docker_image_name

Here, my_docker_container and downloaded_docker_image_name are names used only for reference.
You need to replace them with actual names.

### 3. Observe the Logs of a Docker Service for Maintenance:

In order to see the logs of a specific Docker service, we can use the command below:
docker compose logs -f docker_service_name

Here, docker_service_name is a name used only for reference. You need to replace this name
with the specific actual Docker service which you want to observe the logs for.

### 4. Observe the Docker Containers Running Under a Docker Service:

In order to see the names of the Docker containers running under a Docker service, we can use the command below:
docker compose ps docker_service_name

### Accessing the UI

| Service   | URL                   | What you see                                 |
| --------- | --------------------- | -------------------------------------------- |
| Dashboard | http://localhost:5050 | Live taxi map, fleet stats, incident lists   |
| Flink UI  | http://localhost:8081 | Stream processing job, operators, throughput |

### Verifying Kafka

```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic taxi-data \
  --from-beginning
```

Expected output:

```json
{
  "taxiId": 10,
  "timestamp": "2008-02-08 12:03:42",
  "lon": 116.32823,
  "lat": 39.96615
}
```

### Verifying Flink

Open `http://localhost:8081` → **Running Jobs**. You should see a job called `Beijing Taxi Monitoring System` with four operators all showing `RUNNING`.

You can also access the Flink containers directly from your terminal:

```bash
# open a shell in the JobManager container
docker compose exec flink-jobmanager bash

# View last 100 lines in logs
docker compose logs flink-jobmanager --tail 100
```

### Verifying Redis

```bash
docker-compose exec redis redis-cli
keys taxi:*

# pick one existing taxi key from the output above
hgetall <existing_taxi_key>
```

Example:

```
127.0.0.1:6379> keys taxi:*
1) "taxi:42"
2) "taxi:187"

127.0.0.1:6379> hgetall taxi:42
taxiId              42
timestamp           2008-02-08 15:51:31
lon                 116.54723
lat                 39.90841
speed               13.04
area_violation      1
speed_violation     0
total_distance_km   265.026
avg_speed           14.82
```

If `keys taxi:*` returns an empty result, there is currently no active taxi state in Redis (for example, after stream completion, `END` cleanup, or a Redis restart).

### Accessing PostgreSQL service:

```bash
# Show only containers related to the postgres service
docker compose ps postgres

# Open PostgreSQL shell inside the postgres container
docker compose exec postgres psql -U taxi -d taxi_db

# Run required queries from host terminal
docker compose exec postgres psql -U taxi -d taxi_db -c "\dt"
docker compose exec postgres psql -U taxi -d taxi_db -c "SELECT COUNT(*) FROM taxi_events;"
docker compose exec postgres psql -U taxi -d taxi_db -c "SELECT taxi_id, created_at FROM taxi_events ORDER BY created_at DESC LIMIT 5;"
```

### Adjusting Replay Speed

You can change the `SPEED_FACTOR` in the `docker-compose.yml` file under the `producer` service

### Viewing Logs

```bash
docker-compose logs -f flink-processor
docker-compose logs -f producer
```

## Support

For issues with this project contact the team via their TUHH email

## Roadmap

**Stage 1 — completed:**

- Docker infrastructure setup
- Kafka producer with configurable replay speed
- Flink stream processing operators

**Stage 2 — completed:**

- Finishing up Redis
- Web dashboard with live map and real data
- Performance benchmarking — measuring end-to-end delay at different speeds
- Cloud deployment on AWS

## Contributing:

We are open to more contributions. Our requirements for accepting them are clearly-written and maintainable code and documentation contributions.

## Authors and Acknowledgment

**Team BD26_Project_T1_B** — Institute for Data Engineering, TUHH

## License

Academic Non-Commercial License (ANCL) v1.0

Copyright (c) 2026 Team BD26_Project_T1_B
Institute for Data Engineering, TUHH

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to use,
study, and modify the Software for non-commercial and academic purposes,
subject to the following conditions:

1. Attribution
   Any use of the Software or substantial portions of it must retain this
   copyright notice, this license text, and appropriate attribution to:
   "Team BD26_Project_T1_B, Institute for Data Engineering, TUHH".

2. Non-Commercial Use Only
   The Software may not be used, in whole or in part, for commercial purposes.
   Commercial purposes include, but are not limited to, selling, licensing,
   sublicensing, offering paid services based on the Software, or integrating
   the Software into commercial products or services.

3. Redistribution
   You may redistribute the Software in source form, with or without
   modifications, provided that:
   - this license and attribution are included, and
   - redistribution remains non-commercial.

4. Derivative Works
   You may create derivative works for non-commercial purposes only. Any
   derivative work distributed to others must include this same license and
   attribution.

5. No Warranty
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT, OR OTHERWISE, ARISING
   FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
   DEALINGS IN THE SOFTWARE.

6. Termination
   Any rights granted under this license terminate automatically if you fail to
   comply with its terms.

