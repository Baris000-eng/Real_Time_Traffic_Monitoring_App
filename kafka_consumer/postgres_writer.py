import json
import os
import time

import psycopg2

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "taxi_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "taxi")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "taxi123")


def create_postgres_connection():
    """
    Create a PostgreSQL connection and retry until the database
    becomes available.
    """
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            print("Connected to PostgreSQL")
            return conn

        except psycopg2.OperationalError as exc:
            print(f"PostgreSQL not available: {exc}")
            time.sleep(5)


def ensure_postgres_schema(conn):
    """
    Create the table and indexes needed for storing processed
    Kafka/Flink taxi events.

    Parameters
    ----------
    conn : psycopg2.connection
        Active database connection.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS taxi_events (
                id BIGSERIAL PRIMARY KEY,
                taxi_id TEXT NOT NULL,
                event_data JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_taxi_events_taxi_id
            ON taxi_events (taxi_id);
            """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_taxi_events_created_at
            ON taxi_events (created_at);
            """)

    conn.commit()
    print("PostgreSQL schema verified")


def write_to_postgres(conn, data):
    """
    Insert a processed taxi event into PostgreSQL.

    Parameters
    ----------
    conn : psycopg2.connection
        Active database connection.
    data : dict
        Processed event received from Kafka.
    """
    try:
        taxi_id = str(data.get("taxiId") or data.get("taxi_id") or "unknown")

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO taxi_events (taxi_id, event_data)
                VALUES (%s, %s)
                """,
                (
                    taxi_id,
                    json.dumps(data, default=str),
                ),
            )

        conn.commit()

    except Exception as exc:
        conn.rollback()
        print(f"PostgreSQL Write Error: {exc}")


def close_postgres_connection(conn):
    """Close PostgreSQL connection safely.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Active database connection.
    """
    if conn:
        conn.close()
        print("PostgreSQL connection closed")
