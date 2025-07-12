from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

def transform_iqair_data(**kwargs):
    logger = LoggingMixin().log
    raw = kwargs["ti"].xcom_pull(task_ids="extract_iqair_data", key="raw_data")
    now = datetime.utcnow()
    stations = []

    for marker in raw.get("markers", []):
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
    
    logger.info(f"📦 Transformed {len(stations)} records.")
    kwargs["ti"].xcom_push(key="stations", value=stations)
