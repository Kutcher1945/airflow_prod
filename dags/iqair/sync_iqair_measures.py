from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm
import time

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DB_CONFIG = {
    "host": "10.100.200.150",
    "port": "5439",
    "dbname": "sitcenter_postgis_datalake",
    "user": "la_noche_estrellada",
    "password": "Cfq,thNb13@"
}

MEASUREMENTS_URL = (
    "https://website-api.airvisual.com/v1/stations/{station_id}/measurements"
    "?units.temperature=celsius&units.distance=kilometer&units.pressure=millibar"
    "&units.system=metric&AQI=US&language=ru"
)

def get_station_ids():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM eco_iq_air_sensors WHERE is_deleted = false")
    ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return ids

def fetch_measurements(station_id):
    try:
        url = MEASUREMENTS_URL.format(station_id=station_id)
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        hourly = data.get("measurements", {}).get("hourly", [])
        daily = data.get("measurements", {}).get("daily", [])

        parsed = []

        for item in hourly:
            parsed.append({
                "station_id": station_id,
                "ts": item.get("ts"),
                "granularity": "hourly",
                "aqi": item.get("aqi"),
                "pm25_aqi": item.get("pm25", {}).get("aqi") if item.get("pm25") else None,
                "pm25_concentration": item.get("pm25", {}).get("concentration") if item.get("pm25") else None,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "is_deleted": False
            })

        for item in daily:
            parsed.append({
                "station_id": station_id,
                "ts": item.get("ts"),
                "granularity": "daily",
                "aqi": item.get("aqi"),
                "pm25_aqi": item.get("pm25", {}).get("aqi") if item.get("pm25") else None,
                "pm25_concentration": item.get("pm25", {}).get("concentration") if item.get("pm25") else None,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "is_deleted": False
            })

        return parsed
    except Exception as e:
        print(f"❌ Error fetching for {station_id}: {e}")
        return []

def insert_measurements(records):
    if not records:
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
    print(f"✅ Upserted {len(records)} records")

def run_etl():
    station_ids = get_station_ids()
    all_measurements = []

    for station_id in tqdm(station_ids, desc="Fetching measurements"):
        records = fetch_measurements(station_id)
        all_measurements.extend(records)
        time.sleep(0.2)

    insert_measurements(all_measurements)

with DAG(
    "iqair_measurements",
    default_args=default_args,
    description="Fetch and store air quality data from external API into PostgreSQL",
    schedule_interval="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2025, 7, 16),
    catchup=False,
    tags=["air_quality", "etl", "eco_iq"]
) as dag:

    etl_task = PythonOperator(
        task_id="fetch_and_insert_air_data",
        python_callable=run_etl,
    )

    etl_task
