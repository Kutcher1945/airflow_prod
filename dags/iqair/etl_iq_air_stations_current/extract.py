import psycopg2
import logging
import traceback
from config.db_configs import DB_CONFIG  # Ensure this is under dags/config

def extract_station_ids():
    """
    Extracts active station IDs from the eco_iq_air_sensors table
    where is_deleted = false.

    Returns:
        List[int]: List of station IDs
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM eco_iq_air_sensors WHERE is_deleted = false")
                station_ids = [row[0] for row in cursor.fetchall()]
                logging.info(f"🔍 Extracted {len(station_ids)} station IDs")
                return station_ids

    except Exception as e:
        logging.error("❌ Extract failed: %s", e)
        logging.debug(traceback.format_exc())
        raise  # You could return [] here if you want to suppress failure
