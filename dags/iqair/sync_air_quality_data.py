import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

DB_CONFIG = {
    "host": "10.100.200.150",
    "port": "5439",
    "dbname": "sitcenter_postgis_datalake",
    "user": "la_noche_estrellada",
    "password": "Cfq,thNb13@"
}

CLUSTERS_URL = (
    "https://website-api.airvisual.com/v1/places/map/clusters"
    "?bbox=76.34,42.93,77.45,43.53"
    "&zoomLevel=10"
    "&units.temperature=celsius"
    "&units.distance=kilometer"
    "&units.pressure=millibar"
    "&units.system=metric"
    "&AQI=US"
    "&language=ru"
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_station_data():
    resp = requests.get(CLUSTERS_URL)
    resp.raise_for_status()
    data = resp.json()
    now = datetime.utcnow()
    stations = []

    for marker in data.get("markers", []):
        if marker.get("type") != "station":
            continue
        coords = marker.get("coordinates", {})
        stations.append({
            "id": marker["id"],
            "name": marker.get("name"),
            "latitude": coords.get("latitude"),
            "longitude": coords.get("longitude"),
            "aqi": marker.get("aqi"),
            "url": marker.get("url"),
            "is_high_precision": marker.get("isHighPrecisionStation", False),
            "type": marker.get("type", "station"),
            "created_at": now,
            "updated_at": now,
            "is_deleted": False,
            "dag_updated": True,
            "dag_last_updated_at": now
        })
    return stations

def get_existing_ids(cursor):
    cursor.execute("SELECT id FROM eco_iq_air_sensors")
    return set(row[0] for row in cursor.fetchall())

def sync_to_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Add new columns if they don't exist
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

    # Optional: reset dag_updated flags
    cursor.execute("UPDATE eco_iq_air_sensors SET dag_updated = FALSE")

    stations = fetch_station_data()
    existing_ids = get_existing_ids(cursor)

    new = [s for s in stations if s["id"] not in existing_ids]
    existing = [s for s in stations if s["id"] in existing_ids]
    logging.info(f"🔄 Inserting {len(new)} new and {len(existing)} updated stations...")

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
    logging.info("✅ Sync complete.")

# Define the DAG
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="sync_iqair_air_sensors",
    description="Sync IQAir air quality stations into PostgreSQL every 10 minutes",
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["iqair", "air_quality"]
) as dag:

    sync_task = PythonOperator(
        task_id="sync_iqair_stations",
        python_callable=sync_to_postgres
    )

    sync_task
