import os
import json
from collections import OrderedDict
from datetime import datetime
from pyflink.common.typeinfo import Types
from calculate_haversine import calculate_haversine
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction

# Local default switch for snap-to-road behavior.
# Set this to True/False in this file when you want a code-level toggle.
USE_SNAP_TO_ROAD_DEFAULT = False

USE_SNAP_TO_ROAD = USE_SNAP_TO_ROAD_DEFAULT
MAP_FILE = "beijing_roads.graphml"

class TaxiProcessor(KeyedProcessFunction):
    """
    KeyedProcessFunction for processing taxi location data.

    This class handles snap-to-road functionality, caching, and state management
    for each taxi's last known position.
    """

    CENTER_LAT = 39.917
    CENTER_LON = 116.397
    SNAP_CACHE_MAX_SIZE = 5000
    SNAP_CACHE_PRECISION = 4

    def __init__(self):
        self.last_pos_state = None
        self.snap_enabled = USE_SNAP_TO_ROAD
        self.tree = None
        self.lines = []
        self.snap_cache = OrderedDict()
        self.Point = None

    def open(self, runtime_context):
        state_desc = ValueStateDescriptor("last_pos", Types.PICKLED_BYTE_ARRAY())
        self.last_pos_state = runtime_context.get_state(state_desc)

        if not self.snap_enabled:
            return

        try:
            import osmnx as ox
            from shapely.geometry import Point, LineString
            from shapely.strtree import STRtree

            if not os.path.exists(MAP_FILE):
                graph = ox.graph_from_point((39.9167, 116.3975), dist=15000, network_type="drive")
                ox.save_graphml(graph, MAP_FILE)

            graph = ox.load_graphml(MAP_FILE)

            for u, v, data in graph.edges(data=True):
                if "geometry" in data:
                    self.lines.append(data["geometry"])
                else:
                    node_u = graph.nodes[u]
                    node_v = graph.nodes[v]
                    self.lines.append(LineString([(node_u["x"], node_u["y"]), (node_v["x"], node_v["y"])]))

            self.tree = STRtree(self.lines)
            self.Point = Point
        except Exception as e:
            print(f"Snap-to-road disabled due to initialization error: {e}")
            self.snap_enabled = False

    def snap_to_road(self, lat, lon):
        if not self.snap_enabled or self.tree is None or self.Point is None:
            return lat, lon

        cache_key = self.get_snap_cache_key(lat, lon)
        cached_coords = self.snap_cache.get(cache_key)
        if cached_coords is not None:
            self.snap_cache.move_to_end(cache_key)
            return cached_coords

        point = self.Point(lon, lat)
        nearest = self.tree.nearest(point)

        if nearest is None:
            return lat, lon

        if hasattr(nearest, "__index__"):
            nearest_line = self.lines[int(nearest)]
        else:
            nearest_line = nearest

        projection = nearest_line.project(point)
        snapped_point = nearest_line.interpolate(projection)

        snapped_coords = (snapped_point.y, snapped_point.x)
        self.snap_cache[cache_key] = snapped_coords
        if len(self.snap_cache) > self.SNAP_CACHE_MAX_SIZE:
            self.snap_cache.popitem(last=False)

        return snapped_coords

    def get_snap_cache_key(self, lat, lon):
        return (
            round(lat, self.SNAP_CACHE_PRECISION),
            round(lon, self.SNAP_CACHE_PRECISION),
        )

    def process_element(self, value, ctx):
        data = self.parse_json(value)
        if not data:
            return

        end_payload = self.handle_end(data)
        if end_payload is not None:
            yield end_payload
            return

        parsed = self.parse_input(data)
        if not parsed:
            return

        curr_lat, curr_lon, curr_time = parsed
        prev_data = self.last_pos_state.value()

        if prev_data and not self.is_valid_time(curr_time, prev_data['timestamp']):
            return

        if self.snap_enabled:
            snapped_lat, snapped_lon = self.snap_to_road(curr_lat, curr_lon)
            if snapped_lat is None or snapped_lon is None:
                return
            curr_lat, curr_lon = snapped_lat, snapped_lon
        data['lat'] = curr_lat
        data['lon'] = curr_lon

        if not prev_data:
            self.update_state(curr_lat, curr_lon, curr_time, was_in_violation=False)
            return

        prev_lat = prev_data['lat']
        prev_lon = prev_data['lon']
        prev_time = prev_data['timestamp']
        had_violation_before = prev_data.get('was_in_violation', False)
        dist = self.compute_distance(prev_lat, prev_lon, curr_lat, curr_lon)
        speed = self.compute_speed(dist, curr_time, prev_time)
        dist_to_center = self.compute_distance(curr_lat, curr_lon, self.CENTER_LAT, self.CENTER_LON)

        if self.is_impossible_spatial_jump(dist) or self.is_gps_noise(dist) or self.is_unrealistic_speed(speed):
            self.update_state(curr_lat, curr_lon, curr_time, had_violation_before)
            yield json.dumps(
                self.build_output(
                    data,
                    speed=0.0,
                    dist_to_center=dist_to_center,
                    had_violation_before=had_violation_before,
                    speed_violation=0,
                )
            )
            return

        result = self.build_valid_result(data, speed, dist_to_center, had_violation_before)

        if result:
            yield json.dumps(result)
            new_violation_status = bool(result["area_violation"])
            self.update_state(curr_lat, curr_lon, curr_time, new_violation_status)
        else:
            self.update_state(curr_lat, curr_lon, curr_time, had_violation_before)

    def parse_json(self, value):
        try:
            return json.loads(value)
        except Exception:
            return None

    def handle_end(self, data):
        if data.get("status") == "END":
            self.last_pos_state.clear()
            return json.dumps(
                {
                    "taxiId": data.get("taxiId"),
                    "status": "END",
                    "timestamp": data.get("timestamp"),
                }
            )
        return None

    def parse_input(self, data):
        try:
            lat = float(data.get('lat'))
            lon = float(data.get('lon'))
            ts = datetime.strptime(data.get('timestamp'), "%Y-%m-%d %H:%M:%S")
            return lat, lon, ts.timestamp()
        except Exception:
            return None

    def is_valid_time(self, curr_time, prev_time):
        return curr_time > prev_time and curr_time > 0 and prev_time > 0

    def compute_distance(self, lat1, lon1, lat2, lon2):
        if None in (lat1, lon1, lat2, lon2):
            return 0.0

        return calculate_haversine(lat1, lon1, lat2, lon2)

    def is_impossible_spatial_jump(self, dist):
        return dist >= 5.0

    def is_gps_noise(self, dist):
        return dist <= 0.01

    def compute_speed(self, dist, curr_time, prev_time):
        time_diff = (curr_time - prev_time) / 3600.0
        if time_diff <= 0:
            return 0.0
        if dist < 0:
            return 0.0
        return dist / time_diff

    def is_unrealistic_speed(self, speed):
        return speed <= 0 or speed > 110.0

    def build_valid_result(self, data, speed, dist_to_center, had_violation_before):
        if dist_to_center > 15:
            return None

        return self.build_output(
            data,
            speed=speed,
            dist_to_center=dist_to_center,
            had_violation_before=had_violation_before,
            speed_violation=int(speed > 50),
        )

    def build_output(self, data, speed, dist_to_center, had_violation_before, speed_violation):
        return {
            **data,
            "speed": round(speed, 2),
            "distance_to_center_km": round(dist_to_center, 2),
            "area_violation": self.compute_area_violation(dist_to_center, had_violation_before),
            "speed_violation": speed_violation,
        }

    def compute_area_violation(self, dist_to_center, had_violation_before):
        if had_violation_before:
            return 1 if dist_to_center >= 9.8 else 0
        return 1 if dist_to_center > 10.0 else 0

    def update_state(self, lat, lon, timestamp, was_in_violation):
        self.last_pos_state.update({
            'lat': lat,
            'lon': lon,
            'timestamp': timestamp,
            'was_in_violation': was_in_violation
        })