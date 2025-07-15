from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from iqair.etl_iq_air_sensors.extract import extract_iqair_data
from iqair.etl_iq_air_sensors.transform import transform_iqair_data
from iqair.etl_iq_air_sensors.load import load_to_postgres


default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="sync_iqair_air_sensors",
    description=(
        'This ETL DAG syncs air quality data from the IQAir API into a PostgreSQL database every 10 minutes. '
        'The DAG includes the following steps:\n'
        '- **Extract**: Fetches raw air quality data from the IQAir API for active stations.\n'
        '- **Transform**: Processes the raw API data, extracting key metrics such as AQI, temperature, humidity, '
        'pressure, and wind speed/direction for each station.\n'
        '- **Load**: Loads the transformed data into the PostgreSQL database, ensuring that the latest data is saved '
        'into both the current and historical tables.\n\n'
        'This DAG runs every 10 minutes, keeping the database updated with the most recent air quality information. '
        'The ETL process is designed with automatic retries and failure handling to ensure robust operation. '
        'The task flow is sequential, with the **Extract** task executed first, followed by **Transform**, and finally **Load**.'
    ),
    schedule_interval="*/10 * * * *",  # Every 10 minutes
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["iqair", "air_quality"]
) as dag:

    extract = PythonOperator(
        task_id="extract_iqair_data",
        python_callable=extract_iqair_data
    )

    transform = PythonOperator(
        task_id="transform_iqair_data",
        python_callable=transform_iqair_data
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    # Setting task dependencies
    extract >> transform >> load