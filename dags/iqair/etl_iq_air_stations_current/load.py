import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import logging
from config.db_configs import DB_CONFIG

def load_station_data(records):
    if not records:
        logging.info("⛔ No records to insert.")
        return

    parsed_records = [{**r,
        "created_at": datetime.fromisoformat(r["created_at"]),
        "updated_at": datetime.fromisoformat(r["updated_at"])
    } for r in records]
    now = datetime.utcnow()
    history_records = [{**r, "logged_at": now} for r in parsed_records]

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO eco_iq_air_sensors_current (
            station_id, ts, aqi, temperature, pressure, humidity,
            condition, wind_speed, wind_direction, created_at,
            updated_at, is_deleted
        ) VALUES (
            %(station_id)s, %(ts)s, %(aqi)s, %(temperature)s, %(pressure)s,
            %(humidity)s, %(condition)s, %(wind_speed)s, %(wind_direction)s,
            %(created_at)s, %(updated_at)s, %(is_deleted)s
        )
        ON CONFLICT (station_id, ts) DO UPDATE SET
            aqi = EXCLUDED.aqi,
            temperature = EXCLUDED.temperature,
            pressure = EXCLUDED.pressure,
            humidity = EXCLUDED.humidity,
            condition = EXCLUDED.condition,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction = EXCLUDED.wind_direction,
            updated_at = EXCLUDED.updated_at,
            is_deleted = FALSE
    """

    history_sql = """
        INSERT INTO eco_iq_air_sensors_current_history (
            station_id, ts, aqi, temperature, pressure, humidity,
            condition, wind_speed, wind_direction, original_created_at,
            original_updated_at, is_deleted, logged_at
        ) VALUES (
            %(station_id)s, %(ts)s, %(aqi)s, %(temperature)s, %(pressure)s,
            %(humidity)s, %(condition)s, %(wind_speed)s, %(wind_direction)s,
            %(created_at)s, %(updated_at)s, %(is_deleted)s, %(logged_at)s
        )
    """

    execute_batch(cursor, insert_sql, parsed_records, page_size=50)
    execute_batch(cursor, history_sql, history_records, page_size=50)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"✅ Loaded {len(parsed_records)} records.")
