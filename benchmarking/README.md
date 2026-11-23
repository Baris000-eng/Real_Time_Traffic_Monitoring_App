## Performance Benchmarking

### What We Measure

For benchmarking we measure the end-to-end latency (T2 - T1) of the pipeline.

- **T1** is the time when a GPS event is sent from the Kafka producer. This timestamp is stored as `ingestion_time` in `producer.py`.
- **T2** is the time when the event finishes processing in Flink and is written to Redis. This timestamp is recorded in `redis_consumer.py` right before calling `write_to_redis`.

For every GPS event, we calculate:

```
Latency = T2 - T1
```

The latency (in milliseconds) is stored in a Redis list called `delay_log`. After each benchmark, the average latency is calculated from the values stored in this list.

### Flood Mode

Normally, the producer replays the dataset using the original timestamps so that the taxis move at a realistic speed.

For benchmarking, this behavior is disabled by enabling `FLOOD_MODE`.

When `FLOOD_MODE=true`, the producer sends messages as fast as possible without waiting between timestamps. This creates a heavy workload so we can measure how the pipeline performs under increasing load.

For normal demonstrations, `FLOOD_MODE` should be set back to `"false"`

### Step 1 — Create the Benchmark Datasets

This only needs be done once

The folders below contain subsets of the full T-Drive dataset and are used to test different workloads.

**On Mac / Linux:**

```bash
mkdir -p data-provider/data-100
mkdir -p data-provider/data-1000
mkdir -p data-provider/data-5000

ls data-provider/data/*.txt | head -100 | xargs -I {} cp {} data-provider/data-100/
ls data-provider/data/*.txt | head -1000 | xargs -I {} cp {} data-provider/data-1000/
ls data-provider/data/*.txt | head -5000 | xargs -I {} cp {} data-provider/data-5000/

# Check the number of files
ls data-provider/data-100/ | wc -l
ls data-provider/data-1000/ | wc -l
ls data-provider/data-5000/ | wc -l
```

**On Windows (PowerShell):**

```powershell
New-Item -ItemType Directory -Path "data-provider\data-100"
New-Item -ItemType Directory -Path "data-provider\data-1000"
New-Item -ItemType Directory -Path "data-provider\data-5000"

$files = Get-ChildItem "data-provider\data\*.txt"

$files | Select-Object -First 100 | ForEach-Object { Copy-Item $_.FullName "data-provider\data-100\" }
$files | Select-Object -First 1000 | ForEach-Object { Copy-Item $_.FullName "data-provider\data-1000\" }
$files | Select-Object -First 5000 | ForEach-Object { Copy-Item $_.FullName "data-provider\data-5000\" }

# Check the number of files
(Get-ChildItem "data-provider\data-100\").Count
(Get-ChildItem "data-provider\data-1000\").Count
(Get-ChildItem "data-provider\data-5000\").Count
```

These folders are already in `.gitignore` since they are subsets of the T-Drive dataset which is never committed to Git.

The full dataset folder (`data-provider/data/`) is used as the final test and gets the label `10357`.

### Step 2 — Clear Any Old Benchmark Results Before Benchmarking

```bash
cd benchmarking
rm -f benchmark_results.json latency_graph.png
cd ..
```

### Step 3 — Start the Project

```bash
docker-compose down -v
docker-compose up -d --build
```

Wait about 60 seconds for Flink to start.

Then check that the Flink job has been submitted:

```bash
docker-compose logs flink-processor | tail -20
```

You should see a message similar to:

```
Job has been submitted with JobID ...
```

You can also open the Flink dashboard at:

```
http://localhost:8081
```

and verify that the Beijing Taxi Monitoring System job is running

### Step 4 — Run the Benchmarks

Repeat this steps for each dataset:

- 100
- 1000
- 5000
- 10357

---

**4a. Update `docker-compose.yml`**

Enable FLOOD_MODE to true.

Set the SPEED_FACTOR to 100.

Example for the 100 taxi dataset:

```yaml
producer:
  environment:
    SPEED_FACTOR: "100"
    FLOOD_MODE: "true"
  volumes:
    - ./data-provider/data-100:/data
```

For the full dataset, the volume line should be:

```yaml
- ./data-provider/data:/data
```

---

**4b. Clear both Kafka topics and Redis delay_log:**

```bash
docker-compose exec kafka kafka-topics --delete --topic taxi-data --bootstrap-server localhost:9092
docker-compose exec kafka kafka-topics --create --topic taxi-data --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

docker-compose exec kafka kafka-topics --delete --topic processed-taxi-data --bootstrap-server localhost:9092
docker-compose exec kafka kafka-topics --create --topic processed-taxi-data --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

docker-compose exec redis redis-cli del delay_log
```

Note: we use `--partitions 2` to match the `KAFKA_NUM_PARTITIONS: 2` setting in docker-compose.yml. Using a different partition count during tests would produce unfair results.

---

**4c. Rebuild and start just the producer:**

```bash
docker-compose up -d --build producer
```

Check that the producer has started:

```bash
docker-compose logs producer
```

---

**4d. Wait for Flink to process the data**

For the 100, 1000 and 5000 taxi datasets, waiting about **120 seconds** was enough during testing.

```bash
sleep 120
docker-compose exec redis redis-cli llen delay_log
```

For the full dataset (10357 taxis), loading the dataset takes much longer because it contains over 17 million GPS events.

---

**4e. Save the Benchmark Result**

```bash
cd benchmarking
python3 benchmark.py 100
cd ..
```

Replace `100` with the correct label for the dataset being tested.

---

**4f. Stop the producer and clean up before the next test:**

```bash
docker-compose stop producer

docker-compose exec kafka kafka-topics --delete --topic taxi-data --bootstrap-server localhost:9092
docker-compose exec kafka kafka-topics --create --topic taxi-data --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

docker-compose exec kafka kafka-topics --delete --topic processed-taxi-data --bootstrap-server localhost:9092
docker-compose exec kafka kafka-topics --create --topic processed-taxi-data --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

docker-compose exec redis redis-cli del delay_log
```

Repeat Step 4 for the remaining datasets.

### Step 5 — Generate the Graph

Once all four benchmark reults have been collected:

```bash
cd benchmarking
python3 benchmark.py
```

This reads `benchmarking/benchmark_results.json` and saves `benchmarking/latency_graph.png`.

### Step 6 — Return to Normal Demo Mode

After benchmarking is complete, change the producer settings back to normal.

```yaml
producer:
  environment:
    SPEED_FACTOR: "4.0"
    FLOOD_MODE: "false"
  volumes:
    - ./data-provider/data:/data
```

Then restart the producer:

```bash
docker-compose up -d --build producer
```
