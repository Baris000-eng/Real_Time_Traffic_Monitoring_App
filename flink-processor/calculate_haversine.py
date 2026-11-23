import math


def calculate_haversine(lat1, lon1, lat2, lon2):
    """Function calculating the haversine distance, shared by the
    distance_processor.py and taxi_processor.py."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    b = (math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(b), math.sqrt(1 - b))
    return R * c
