import os
import psycopg2
from psycopg2.extras import execute_batch
from airflow.utils.log.logging_mixin import LoggingMixin

DB_CONFIG = {
    "host": os.environ["POSTGRES_HOST"],
    "port": os.environ.get("POSTGRES_PORT", "5432"),
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"]
}

def load_to_postgres(**kwargs):
    logger = LoggingMixin().log
    stations = kwargs["ti"].xcom_pull(task_ids="transform_iqair_data", key="stations")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS eco_iq_air_sensors (
        id TEXT PRIMARY KEY,
        name TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        aqi INT,
        url TEXT,
        is_high_precision BOOLEAN,
        type TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        is_deleted BOOLEAN,
        dag_updated BOOLEAN DEFAULT FALSE,
        dag_last_updated_at TIMESTAMP
    )
    """)
    cursor.execute("UPDATE eco_iq_air_sensors SET dag_updated = FALSE")

    insert_sql = """
    INSERT INTO eco_iq_air_sensors (
        id, name, latitude, longitude, aqi, url,
        is_high_precision, type, created_at, updated_at,
        is_deleted, dag_updated, dag_last_updated_at
    )
    VALUES (
        %(id)s, %(name)s, %(latitude)s, %(longitude)s, %(aqi)s, %(url)s,
        %(is_high_precision)s, %(type)s, %(created_at)s, %(updated_at)s,
        %(is_deleted)s, %(dag_updated)s, %(dag_last_updated_at)s
    )
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        aqi = EXCLUDED.aqi,
        url = EXCLUDED.url,
        is_high_precision = EXCLUDED.is_high_precision,
        type = EXCLUDED.type,
        updated_at = EXCLUDED.updated_at,
        is_deleted = FALSE,
        dag_updated = TRUE,
        dag_last_updated_at = EXCLUDED.dag_last_updated_at
    """
    execute_batch(cursor, insert_sql, stations, page_size=100)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"✅ Loaded {len(stations)} records into the database.")
