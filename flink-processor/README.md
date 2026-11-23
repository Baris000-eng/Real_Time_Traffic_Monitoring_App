# Flink Processor Module

**Part of the BD26_Project_T1_B Real-Time Taxi Fleet Monitor** [⬅ Back to Main Project README](../README.md)

## Overview
The `flink-processor` directory contains the core stream processing engine of our architecture. Built with Apache Flink (PyFlink), this module consumes raw GPS data from the `taxi-data` Kafka topic, cleans and enriches it in real-time, and outputs the refined data to the `processed-taxi-data` topic.

## Key Responsibilities & Operators (short version)

* **Stage 1 - TaxiProcessor:** Validates/parses events, propagates `END` records, applies optional snap-to-road map matching, rejects out-of-order updates, and enforces service-area constraints.
* **Stage 2 - DistanceProcessor:** Maintains per-taxi cumulative `total_distance_km` using Haversine distance between successive points.
* **Stage 3 - AverageSpeedProcessor:** Maintains running average speed per taxi and appends `avg_speed` to emitted events.
* **Stage 4 - LocationFilter5s:** Emits at most one update per taxi every 5 seconds while still passing `END` records immediately.
* **Spatial Indexing (Optional):** STRtree-based nearest-road lookup is available when snap-to-road mode is enabled.

## Execution
This module runs completely within the Flink execution environment. It utilizes a parallelism setting of `2` to distribute the workload across task slots, significantly reducing processing latency.

For detailed instructions on how this module fits into the broader data pipeline or how to run it via Docker, please refer to the [Main README](../README.md).


## Applied stream processing optimizations and configurations for Apache Flink, and their effects on the throughput:

1. Early Data Filtering and Invalid Taxi Record Dropping:

In the taxi_processor.py file of the flink-processor module, several validation and edge-case mechanisms immediately filter out the unwanted data.

1a. Malformed JSON, missing/invalid coordinates, or invalid timestamps are immediately dropped.

1b. If the taxi is in stagnation or exhibits unrealistic movement patterns, such as GPS noise or unrealistic speeds (speed <= 0 km/h or faster than the predefined speed limit of 110.0 km/h), the records are normalized and emitted with the speed set to 0.0 instead of being fully ignored.

1c. If the distance of the taxi to the center is more than the predefined radius limit of 15 km, it will be ignored.

1d. If the current read time of the taxi is smaller than or equal to the previous read time of that taxi, it will be ignored. Additionally, END records are explicitly propagated so downstream operators can seamlessly clear per-taxi state.

Therefore, it will prevent unnecessary computational power from being spent on the unwanted taxi records, which will maximize Apache Flink's downstream processing throughput (number of outgoing taxi records per second), and ensure efficient event collection, processing, and lifecycle consistency for taxi state.

2. Aggressive Level of Upstream Data Reduction/Downsampling:

In the flink-processor module, we have used a location filter mechanism via the "LocationFilter5s" operator in the location_filter.py file to optimize the stream processing. 

In the pipeline configuration within flink_manager.py, the ordering runs sequentially through "TaxiProcessor()", "DistanceProcessor()", and "AverageSpeedProcessor()" before reaching "LocationFilter5s" and the final Kafka sink creation and Redis writer. This ordering ensures that distance and average-speed enrichment are performed on all valid events so they can contribute to stateful calculations, while the downstream emission is throttled. 

The "LocationFilter5s" operator utilizes Apache Flink's memory and Managed Value State (last_emit_state) to limit data transmission, which sends a taxi record if at least 5 seconds have passed since the last sent record (though END records are always passed through immediately). Otherwise, it does not send that taxi record, which prevents excessive data traffic and drastically reduces pressure on consumers and storage sinks.

3. Localization Map Spatial Indexing Mechanism with STRtree

Optimization Applied: In the flink-processor module, within the TaxiProcessor() class inside the taxi_processor.py file, map matching (snapping) the taxi coordinates is optionally applied when enabled (`USE_SNAP_TO_ROAD_DEFAULT` / `USE_SNAP_TO_ROAD`). It is optimized by loading the "beijing_roads.graphml" file into the memory during the initialization phase of the operator and constructing a spatial indexing tree (STRtree) structure.

This optimization increases the throughput by decreasing the time complexity (lookup time) via spatial indexing instead of scanning all road geometries line-by-line. The STRtree structure calculates the nearest road segment efficiently, and snapped coordinates are cached using an LRU-style `OrderedDict` to further reduce repeated computations. 

This tree structure uses bounding boxes to instantly exclude irrelevant geographic regions, isolates the nearest road segment, and utilizes geometric projection to calculate the exact perpendicular distance from the taxi's GPS point to the candidate road line segment, which successfully aligns the coordinates with the true road path. 

4. Division of each job under the execution environment of the flink processor into 2 parallel subtasks: 

For this optimization, we have used 'env.set_parallelism(2)' function call. This parallelism configuration divides the processing workload of each job into 
two seperate parallel subtasks, which executes concurrently in the Apache Flink task slots. By distributing the data streaming workload across these two parallel workers, we have significantly reduced the execution latency per job, and increased the overall throughput of the system. 

5. Watermark strategy of up to 5 seconds of delay in arrival to Apache Flink: 

This strategy allows for a maximum latency of 5 seconds, which means that events can arrive up to 5 seconds late and still be processed in order. This is useful for taxi location 
updates, which may not always arrive in the exact order they were generated, because of network delays. This will improve the system throughput as it will avoid unnecessary 
reprocessing and recomputation of the data stream coming from the past but with a lag. 