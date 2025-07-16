from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from psycopg2.extras import execute_batch
from tqdm import tqdm
import time

# Constants
MEASUREMENTS_URL = (
    "https://website-api.airvisual.com/v1/stations/{station_id}/measurements"
    "?units.temperature=celsius&units.distance=kilometer&units.pressure=millibar"
    "&units.system=metric&AQI=US&language=ru"
)

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# DAG definition
with DAG(
    dag_id='iqair_measurements',
    default_args=default_args,
    description='Fetch and insert measurements from IQAir sensors',
    start_date=datetime(2025, 7, 16),
    schedule_interval='*/10 * * * *',  # every 10 minutes
    catchup=False
) as dag:

    def get_station_ids():
        hook = PostgresHook(postgres_conn_id='iqair_pg')
        sql = "SELECT id FROM eco_iq_air_sensors WHERE is_deleted = false"
        records = hook.get_records(sql)
        return [row[0] for row in records]

    def fetch_measurements(station_id):
        try:
            url = MEASUREMENTS_URL.format(station_id=station_id)
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            hourly = data.get("measurements", {}).get("hourly", [])
            daily = data.get("measurements", {}).get("daily", [])

            parsed = []
            now = datetime.utcnow()

            for item in hourly:
                parsed.append({
                    "station_id": station_id,
                    "ts": item.get("ts"),
                    "granularity": "hourly",
                    "aqi": item.get("aqi"),
                    "pm25_aqi": item.get("pm25", {}).get("aqi") if item.get("pm25") else None,
                    "pm25_concentration": item.get("pm25", {}).get("concentration") if item.get("pm25") else None,
                    "created_at": now,
                    "updated_at": now,
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
                    "created_at": now,
                    "updated_at": now,
                    "is_deleted": False
                })

            return parsed
        except Exception as e:
            print(f"❌ Error fetching for {station_id}: {e}")
            return []

    def insert_measurements(**context):
        hook = PostgresHook(postgres_conn_id='iqair_pg')
        conn = hook.get_conn()
        cursor = conn.cursor()

        station_ids = get_station_ids()
        all_records = []

        for station_id in tqdm(station_ids, desc="Fetching measurements"):
            recs = fetch_measurements(station_id)
            all_records.extend(recs)
            time.sleep(0.2)

        if not all_records:
            print("No records fetched.")
            return

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
        history_records = [{**r, "logged_at": now} for r in all_records]

        execute_batch(cursor, insert_sql, all_records, page_size=100)
        execute_batch(cursor, history_sql, history_records, page_size=100)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Upserted {len(all_records)} records and added to history.")

    run_task = PythonOperator(
        task_id='fetch_and_insert_measurements',
        python_callable=insert_measurements,
        provide_context=True
    )

    run_task
