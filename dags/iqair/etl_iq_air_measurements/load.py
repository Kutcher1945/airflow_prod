import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import json
from config.db_configs import DB_CONFIG 

def load_to_db(**context):
    raw_data = context["ti"].xcom_pull(key="measurements", task_ids="transform_task")
    records = json.loads(raw_data)
    if not records:
        print("No records to insert.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO eco_iq_air_sensors_measurements (
        station_id, ts, granularity, aqi, pm25_aqi, pm25_concentration,
        created_at, updated_at, is_deleted
    ) VALUES (
        %(station_id)s, %(ts)s, %(granularity)s, %(aqi)s, %(pm25_aqi)s, %(pm25_concentration)s,
        %(created_at)s, %(updated_at)s, %(is_deleted)s
    )
    ON CONFLICT (station_id, ts, granularity) DO UPDATE SET
        aqi = EXCLUDED.aqi,
        pm25_aqi = EXCLUDED.pm25_aqi,
        pm25_concentration = EXCLUDED.pm25_concentration,
        updated_at = EXCLUDED.updated_at,
        is_deleted = FALSE
    """

    history_sql = """
    INSERT INTO eco_iq_air_sensors_measurements_history (
        station_id, ts, granularity, aqi, pm25_aqi, pm25_concentration,
        original_created_at, original_updated_at, is_deleted, logged_at
    ) VALUES (
        %(station_id)s, %(ts)s, %(granularity)s, %(aqi)s, %(pm25_aqi)s, %(pm25_concentration)s,
        %(created_at)s, %(updated_at)s, %(is_deleted)s, %(logged_at)s
    )
    """

    now = datetime.utcnow()
    history_records = [{**r, "logged_at": now} for r in records]

    execute_batch(cursor, insert_sql, records, page_size=100)
    execute_batch(cursor, history_sql, history_records, page_size=100)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Loaded {len(records)} records.")
