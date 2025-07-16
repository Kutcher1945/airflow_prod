# dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from iqair.etl_iq_air_measurements.extract import extract_station_ids
from iqair.etl_iq_air_measurements.transform import transform_data
from iqair.etl_iq_air_measurements.load import load_to_db

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "iqair_measurements",
    default_args=default_args,
    description="ETL DAG split into extract, transform, load steps",
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["air_quality", "modular", "etl"]
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_station_ids,
        provide_context=True
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_to_db,
        provide_context=True
    )

    extract >> transform >> load
