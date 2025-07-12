import requests
from airflow.utils.log.logging_mixin import LoggingMixin

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

def extract_iqair_data(**kwargs):
    logger = LoggingMixin().log
    logger.info("🌍 Fetching data from IQAir...")
    resp = requests.get(CLUSTERS_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    kwargs["ti"].xcom_push(key="raw_data", value=data)
