from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from iqair.etl_iq_air_sensors.extract import extract_iqair_data
from iqair.etl_iq_air_sensors.transform import transform_iqair_data
from iqair.etl_iq_air_sensors.load import load_to_postgres


default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="sync_iqair_air_sensors",
    description="ETL for IQAir air quality stations every 10 minutes",
    schedule_interval="*/10 * * * *",
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

    extract >> transform >> load
