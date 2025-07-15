import requests
from datetime import datetime
import logging
from tqdm import tqdm

STATION_DETAIL_URL = (
    "https://website-api.airvisual.com/v1/stations/{station_id}"
    "?fields=livecam,analysis,fires&units.temperature=celsius"
    "&units.distance=kilometer&units.pressure=millibar"
    "&units.system=metric&AQI=US&language=ru"
)

def transform_station_data(station_ids):
    transformed = []

    for station_id in tqdm(station_ids, desc="Transforming station data"):
        try:
            url = STATION_DETAIL_URL.format(station_id=station_id)
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            current = data.get("current", {})
            wind = current.get("wind", {})

            transformed.append({
                "station_id": station_id,
                "ts": current.get("ts"),
                "aqi": current.get("aqi"),
                "temperature": current.get("temperature"),
                "pressure": current.get("pressure"),
                "humidity": current.get("humidity"),
                "condition": current.get("condition"),
                "wind_speed": wind.get("speed"),
                "wind_direction": wind.get("direction"),
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "is_deleted": False
            })
        except Exception as e:
            logging.warning(f"❌ Failed to fetch {station_id}: {e}")
    return transformed
