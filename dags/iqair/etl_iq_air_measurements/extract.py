import psycopg2
from config.db_configs import DB_CONFIG 

def extract_station_ids(**context):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM eco_iq_air_sensors WHERE is_deleted = false")
    ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    context["ti"].xcom_push(key="station_ids", value=ids)
