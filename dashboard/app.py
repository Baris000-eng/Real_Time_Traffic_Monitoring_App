from flask import Flask, render_template
from flask_sock import Sock
import json
import redis
import time

app = Flask(__name__)
socket = Sock(app)

r = redis.Redis(host='redis', port=6379, decode_responses=True)


@app.route("/")
def index():
    return render_template("index.html")

@app.route('/benchmarks')
def show_benchmarks():
    return render_template('benchmarks.html')


@socket.route('/taxi')
def taxi_websocket(ws):
    while True:
        # Fetching all taxi tracking records from Redis state cache
        taxi_keys = r.keys("taxi:*")

        taxis_list = []
        currently_driving = 0
        area_violations = []
        speeding_incidents = []

        for key in taxi_keys:
            taxi_data = r.hgetall(key)
            if taxi_data:
                currently_driving += 1

                taxi_id = int(taxi_data.get('taxiId', 0))
                lat = float(taxi_data.get('lat', 0.0))
                lon = float(taxi_data.get('lon', 0.0))
                distance = float(taxi_data.get('total_distance_km', 0.0))
                speed = float(taxi_data.get('speed', 0.0))

                # Appending full telemetry metrics payload downstream to taxi.js
                speeding_violation = int(taxi_data.get('speed_violation', 0))
                area_violation = int(taxi_data.get('area_violation', 0))
                distance_to_center = float(taxi_data.get('distance_to_center_km', 0.0))

                taxis_list.append({
                    "id": taxi_id,
                    "lat": lat,
                    "lon": lon,
                    "speed": speed,
                    "total_distance_km": round(distance, 2),
                    "area_violation": area_violation,
                    "speed_violation": speeding_violation,
                    "distance_to_center_km": distance_to_center
                })

                if speeding_violation == 1:
                    speeding_incidents.append(taxi_id)

                if area_violation == 1:
                    area_violations.append(taxi_id)

        # Read cumulative overall distance from Redis
        overall_distance = float(r.get("overall_distance") or 0.0)

        payload = {
            "currently_driving": currently_driving,
            "overall_distance": f"{overall_distance:.2f}",
            "area_violations": area_violations,
            "speeding_incidents": speeding_incidents,
            "taxis": taxis_list
        }

        ws.send(json.dumps(payload))
        time.sleep(1)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)