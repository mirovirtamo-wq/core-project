from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os

DATASET_PATH = os.path.expanduser('~/airflow/datasets/weather_data.csv')
DB_PATH = os.path.expanduser('~/airflow/databases/weather_database.db')

def extract(**kwargs):
    df = pd.read_csv(DATASET_PATH)
    temp_file =  '/tmp/weather_temp.csv'
    df.to_csv(temp_file, index=False)
    kwargs["ti"].xcom_push(key='csv_path', value=temp_file)

def transform(**kwargs):
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(key='csv_path', task_ids="extract_task")
    df = pd.read_csv(file_path)
    df["temperature"] = df["temperature"].fillna(df["temperature"].median())
    df["feels_like_temperature"] = df["temperature"] + 0.33*df["humidity"] - 0.7*df["wind_speed"] - 4
    df.to_csv(file_path, index=False)
    ti.xcom_push(key='csv_path', value=file_path)

def load(**kwargs):
   ti = kwargs["ti"]
   file_path = ti.xcom_pull(key='csv_path', task_ids="transform_task")
   df = pd.read_csv(file_path)
   conn = sqlite3.connect(DB_PATH)
   df.to_sql("weather_data", conn, if_exists="replace", index=False)
   conn.close()

default_args = {
    "owner": "airflow", 
    "start_date": days_ago(1),
    "'retries": 1,
}

with DAG(
    dag_id="weather_etl_pipeline", 
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task", 
        python_callable=extract,
    )
   
    transform_task = PythonOperator(
        task_id="transform_task", 
        python_callable=transform
    )
   
    load_task = PythonOperator(
        task_id="load_task", 
        python_callable=load
    )

    extract_task >> transform_task >> load_task
