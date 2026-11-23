import json
from datetime import datetime

from calculate_haversine import calculate_haversine
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor


class DistanceProcessor(KeyedProcessFunction):
    """Stateful operator that aggregates overall distance traveled per taxi
    using the Haversine formula between successive locations.

    Stores per-taxi previous position and accumulated distance in state.
    
    Parameters
    ----------
    KeyedProcessFunction : pyflink.datastream.functions.KeyedProcessFunction
        Base class for implementing keyed process functions in Flink.
    """

    def __init__(self):
        # State to store the previous location of the taxi
        self.last_pos_state = None
        # State to store the cumulative distance in kilometers
        self.total_dist_state = None

    def open(self, runtime_context):
        """
        Initialize Flink states when the operator starts.
        """
        self.last_pos_state = runtime_context.get_state(
            ValueStateDescriptor("dist_last_pos", Types.PICKLED_BYTE_ARRAY())
        )
        self.total_dist_state = runtime_context.get_state(
            ValueStateDescriptor("total_distance", Types.FLOAT())
        )

    def process_element(self, value, ctx):
        """
        Processes each incoming taxi record and updates the total distance.

        Edge Case : If this is the first time we see the taxi,
        we set the distance to 0.0 and send the data downstream.
        This prevents losing the first location record.
        """
        try:
            data = json.loads(value)
        except Exception:
            return

        if data.get("status") == "END":
            self.last_pos_state.clear()
            self.total_dist_state.clear()
            yield json.dumps(data)
            return
        
        curr_time = 0.0 
        
        try:
            ts_str = data.get("timestamp")
            if ts_str:
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                curr_time = dt.timestamp()
            else:
                curr_time = 0.0
        except (ValueError, TypeError):
            curr_time = 0.0

        # Read the current values from Flink state
        prev = self.last_pos_state.value()
        total = self.total_dist_state.value() or 0.0

        curr_lat = data.get("lat")
        curr_lon = data.get("lon")

        if prev and curr_lat is not None and curr_lon is not None:
            dist = calculate_haversine(prev["lat"], prev["lon"], curr_lat, curr_lon)

            total += dist
        else:
            # Edge Case: This is the first record for this taxi (Cold Start).
            # We initialize the total distance to 0.0. We do not drop the record.
            total = 0.0

        self.total_dist_state.update(total)

        data["total_distance_km"] = total

        yield json.dumps(data)

        # update last position for next calculation
        self.last_pos_state.update({"lat": curr_lat, "lon": curr_lon, "timestamp": curr_time})