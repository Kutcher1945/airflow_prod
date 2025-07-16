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