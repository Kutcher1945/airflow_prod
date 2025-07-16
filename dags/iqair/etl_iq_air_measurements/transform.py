import requests
import time
import json
from datetime import datetime
from config.db_configs import MEASUREMENTS_URL

def transform_data(**context):
    station_ids = context["ti"].xcom_pull(key="station_ids", task_ids="extract_task")
    all_measurements = []

    for station_id in station_ids:
        try:
            url = MEASUREMENTS_URL.format(station_id=station_id)
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            hourly = data.get("measurements", {}).get("hourly", [])
            daily = data.get("measurements", {}).get("daily", [])
            now = datetime.utcnow()

            for item in hourly + daily:
                granularity = "hourly" if item in hourly else "daily"
                all_measurements.append({
                    "station_id": station_id,
                    "ts": item.get("ts"),
                    "granularity": granularity,
                    "aqi": item.get("aqi"),
                    "pm25_aqi": item.get("pm25", {}).get("aqi") if item.get("pm25") else None,
                    "pm25_concentration": item.get("pm25", {}).get("concentration") if item.get("pm25") else None,
                    "created_at": now,
                    "updated_at": now,
                    "is_deleted": False
                })
            time.sleep(0.2)
        except Exception as e:
            print(f"❌ Error fetching station {station_id}: {e}")

    context["ti"].xcom_push(key="measurements", value=json.dumps(all_measurements, default=str))
