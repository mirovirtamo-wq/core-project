import sys
import os

sys.path.append(os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract import extract_data

with DAG(
    dag_id="weather_etl",
    start_date=datetime(2023,1,1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task = PythonOperator(task_id="extract", python_callable=extract_data)
