from pyflink.common.watermark_strategy import TimestampAssigner
import json 
import datetime 

class TaxiTimestampAssigner(TimestampAssigner):
    """
    Custom TimestampAssigner for extracting timestamps from taxi location data.
    """
    def extract_timestamp(self, value, record_timestamp):
        try:
            # Parse the JSON 
            data = json.loads(value)
            ts = datetime.strptime(data.get('timestamp'), "%Y-%m-%d %H:%M:%S")
            # Flink expects milliseconds 
            return int(ts.timestamp() * 1000)
        except Exception:
            return record_timestamp # Fallback to system timestamp if parsing fails