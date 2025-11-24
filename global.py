from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import pandas as pd
import os
import sqlite3
import logging

BASE_DIR = os.getenv("AIRFLOW_HOME", "/usr/core/airflow")
DATASETS_DIR = os.path.join(BASE_DIR, "datasets")
FILLED_CSV = os.path.join(DATASETS_DIR, "df_filled_csv")
AVG5_CSV = os.path.join(DATASETS_DIR, "avg5.csv")
RAW_CSV = os.path.join(DATASETS_DIR, "GlobalLandTemperaturesDataset.csv")
TRANSFORMED_CSV = os.path.join(DATASETS_DIR, "transformed_global.csv")
SQLITE_DB = os.path.join(BASE_DIR, "global.db")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS global_land_temperatures (
id INTEGER PRIMARY KEY AUTOINCREMENT,
date DATE,
average_temperature FLOAT,
average_temperature_uncertainty FLOAT,
city VARCHAR(100),
country VARCHAR(100),
latitude VARCHAR(10),
longitude VARCHAR(10)
)
"""

with DAG(
   dag_id="global",
   start_date=days_ago(1),
   schedule_interval=None,
   catchup=False
) as dag:

   def extract(**context):
       if not os.path.exists(RAW_CSV):
           raise FileNotFoundError(f"Dataset not found ar {RAW_CSV}")
       context["ti"].xcom_push(key="raw_path", value=RAW_CSV)

   extract_task = PythonOperator(task_id="extract", python_callable=extract)

   def fill_missing(**context):
       path = context["ti"].xcom_pull(key="raw_path")
       df = pd.read_csv(path)
       df["AverageTemperature"] = df["AverageTemperature"].fillna(df["AverageTemperature"].median())
       df.to_csv(FILLED_CSV, index=False)

   fill_missing_task = PythonOperator(
        task_id="fill_missing",
        python_callable=fill_missing
    )

   def calc_avg_5_years(**context):
       path = context["ti"].xcom_pull(key="raw_path")
       df = pd.read_csv(path)
       df["dt"] = pd.to_datetime(df["dt"])
       latest_year = df["dt"].dt.year.max()
       last_5 = df[df["dt"].dt.year >= latest_year - 5]
       avg5 = last_5.groupby("Country")["AverageTemperature"].mean().reset_index()
       avg5.rename(columns={"AverageTemperature": "avg_temp_last_5_years"}, inplace=True)
       avg5.to_csv(AVG5_CSV, index=False)

   calc_avg_5_task = PythonOperator(task_id="calc_avg_5_years", python_callable=calc_avg_5_years)

   def combine_transform(**context):
       df_filled = pd.read_csv(FILLED_CSV)
       df_avg = pd.read_csv(AVG5_CSV)
       df = df_filled.merge(df_avg, on="Country", how="left")
       df["avg_temp_last_5_years"] = df["avg_temp_last_5_years"].fillna(df["avg_temp_last_5_years"].median())
       df.to_csv(TRANSFORMED_CSV, index=False)
       context["ti"].xcom_push(key="transformed_path", value=TRANSFORMED_CSV)

   combine_task = PythonOperator(task_id="combine_transform", python_callable=combine_transform)

   def validate(**context):
       path = context["ti"].xcom_pull(key="transformed_path", task_ids="combine_transform")
       df = pd.read_csv(path)
       if df["AverageTemperature"].isna().any():
           logging.warning("Validation failed: Missing values found.")
           return False
       if "avg_temp_last_5_years" not in df.columns:
           logging.warning("Validation failed: No column named avg_temp_last_5_years")
           return False
       return True

   validate_task = ShortCircuitOperator(task_id="validate", python_callable=validate)

   def load(**context):
       path = context["ti"].xcom_pull(key="transformed_path", task_ids="combine_transform")
       df = pd.read_csv(path)
       conn = sqlite3.connect(SQLITE_DB)
       df.to_sql("global_land_temperature", conn, if_exists="append", index=False)
       conn.close()

   load_task = PythonOperator(task_id="load", python_callable=load, trigger_rule=TriggerRule.ALL_SUCCESS)

   extract_task >> [fill_missing_task, calc_avg_5_task] >> combine_task  >> validate_task >> load_task
