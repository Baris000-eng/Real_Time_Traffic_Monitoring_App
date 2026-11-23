import os
import redis


def create_redis_client():
    """This function creates and returns a Redis client instance.

    It connects to the Redis service using host and port values defined
    via environment variables.
    """
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )


def write_to_redis(client, data):
    """This function writes a processed taxi record into Redis.

    It stores the data as a Redis hash using the taxiId as part of the
    key. Also updates the cumulative overall distance metric.

    Parameters
    ----------
    client : redis.Redis
        The Redis client instance.
    data : dict
        The processed taxi record to be stored in Redis.
    """
    try:
        taxi_id = data.get("taxiId")

        if taxi_id is None:
            raise ValueError("Missing taxiId in record")

        mapping = {key: str(val) for key, val in data.items() if val is not None}

        # Get the previous distance for this taxi to calculate delta
        prev_taxi_data = client.hgetall(f"taxi:{taxi_id}")
        prev_distance = float(prev_taxi_data.get("total_distance_km", 0.0))

        # New distance from the data
        curr_distance = float(data.get("total_distance_km", 0.0))

        # Calculate distance delta and add to overall cumulative distance
        distance_delta = curr_distance - prev_distance
        if distance_delta > 0:
            client.incrbyfloat("overall_distance", distance_delta)

        client.hset(f"taxi:{taxi_id}", mapping=mapping)

    except Exception as e:
        print(f"Redis Write Error: {e}")


def delete_from_redis(client, taxi_id):
    """Delete a taxi hash from Redis when an END message is received.

    Note: The overall_distance metric is preserved; it is NOT decremented.
    """
    try:
        if taxi_id is None:
            raise ValueError("Missing taxiId in END record")

        client.delete(f"taxi:{taxi_id}")

    except Exception as e:
        print(f"Redis Delete Error: {e}")
