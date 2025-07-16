from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from iqair.etl_iq_air_stations_current.extract import extract_station_ids
from iqair.etl_iq_air_stations_current.transform import transform_station_data
from iqair.etl_iq_air_stations_current.load import load_station_data

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'iqair_sensors_history',
    default_args=default_args,
    description=(
        'This ETL DAG synchronizes live air quality data from external API sources '
        'into the "eco_iq_air_sensors_current" and "eco_iq_air_sensors_current_history" '
        'tables in the PostgreSQL database. It follows a modular ETL structure with '
        'three tasks: \n'
        '- Extract: Fetches active station IDs from the database.\n'
        '- Transform: Retrieves and processes detailed sensor data for each station from the API.\n'
        '- Load: Inserts the transformed data into current and historical sensor data tables.\n\n'
        'The DAG runs every 5 minutes to ensure near real-time updates of air quality sensor readings, '
        'including AQI, temperature, pressure, humidity, and wind metrics. It is designed with retries and '
        'failure resilience, and uses XCom for passing intermediate data between tasks.'
    ),
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 7, 15),
    catchup=False
)

def extract_task(ti):
    station_ids = extract_station_ids()
    ti.xcom_push(key='station_ids', value=station_ids)

def transform_task(ti):
    station_ids = ti.xcom_pull(key='station_ids', task_ids='extract_task')
    data = transform_station_data(station_ids)
    ti.xcom_push(key='transformed_data', value=data)

def load_task(ti):
    records = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
    load_station_data(records)

# Define Airflow tasks
t1 = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    dag=dag
)

t2 = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    dag=dag
)

t1 >> t2 >> t3
