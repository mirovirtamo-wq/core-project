from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

import pandas as pd
import os
import sqlite3
import logging
from datetime import datetime

BASE_DIR = os.getenv("AIRFLOW_HOME", "/usr/core/airflow")  
DATASETS_DIR = os.path.join(BASE_DIR, "datasets")
RAW_CSV = os.path.join(DATASETS_DIR, "global_power_plant_database.csv")
TRANSFORMED_CSV = os.path.join(DATASETS_DIR, "transformed_power_plants.csv")
SQLITE_DB = os.path.join(BASE_DIR, "power_plant.db")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS power_plant_data (
    country TEXT,
    country_long TEXT,
    name TEXT,
    gppd_idnr TEXT,
    capacity_mw REAL,
    latitude REAL,
    longitude REAL,
    primary_fuel TEXT,
    other_fuel1 TEXT,
    other_fuel2 TEXT,
    other_fuel3 TEXT,
    commissioning_year REAL,
    owner TEXT,
    source TEXT,
    url TEXT,
    geolocation_source TEXT,
    wepp_id TEXT,
    year_of_capacity_data REAL,
    generation_gwh_2013 REAL,
    generation_gwh_2014 REAL,
    generation_gwh_2015 REAL,
    generation_gwh_2016 REAL,
    generation_gwh_2017 REAL,
    generation_gwh_2018 REAL,
    generation_gwh_2019 REAL,
    generation_data_source TEXT,
    estimated_generation_gwh_2013 REAL,
    estimated_generation_gwh_2014 REAL,
    estimated_generation_gwh_2015 REAL,
    estimated_generation_gwh_2016 REAL,
    estimated_generation_gwh_2017 REAL,
    estimated_generation_note_2013 TEXT,
    estimated_generation_note_2014 TEXT,
    estimated_generation_note_2015 TEXT,
    estimated_generation_note_2016 TEXT,
    estimated_generation_note_2017 TEXT,
    age_of_plant REAL
);
"""

default_args = {"owner":"student",
"retries":0}

with DAG(
   dag_id="power_plant_et1",
   default_args=default_args,
   start_date=days_ago(1),
   schedule_interval=None,
) as dag:

   def create_db():
       os.makedirs(DATASETS_DIR, exist_ok=True)
       conn = sqlite3.connect(SQLITE_DB)
       conn.execute(CREATE_TABLE_SQL)
       conn.commit()
       conn.close()

   create_db_task = PythonOperator(task_id="create_db", python_callable=create_db)

   def extract(**context):
       if not os.path.exists(RAW_CSV):
           raise FileNotFoundError(f"Dataset not found at {RAW_CSV}")
       context["ti"].xcom_push(key="raw_path", value=RAW_CSV)

   extract_task = PythonOperator(task_id="extract", python_callable=extract)

   def transform(**context):
       path = context["ti"].xcom_pull(key="raw_path", task_ids="extract")
       df = pd.read_csv(path)
       if "capacity_mw" in df.columns:
           df["capacity_mw"].fillna(df["capacity_mw"].median(), inplace=True)
       if "commissioning_year" in df.columns:
           df["commissioning_year"].fillna(df["commissioning_year"].median(), inplace=True)
           df["age_of_plant"] = datetime.now().year - df["commissioning_year"]
       df.to_csv(TRANSFORMED_CSV, index=False)
       context["ti"].xcom_push(key="transformed_path", value=TRANSFORMED_CSV)

   transform_task = PythonOperator(task_id="transform", python_callable=transform)

   def validate(**context):
       path = context["ti"].xcom_pull(key="transformed_path", task_ids="transform")
       df = pd.read_csv(path)
       if df["capacity_mw"].isna().any() or df["commissioning_year"].isna().any():
           logging.warning("Validation failed: Missing values found.")           
           return False
       return True

   validate_task = ShortCircuitOperator(task_id="validate", python_callable=validate)

   def load(**context):
       path = context["ti"].xcom_pull(key="transformed_path", task_ids="transform")
       df = pd.read_csv(path)
       conn = sqlite3.connect(SQLITE_DB)
       df.to_sql("power_plant_data", conn, if_exists="append", index=False)
       conn.close()

   load_task = PythonOperator(task_id="load", python_callable=load)

   create_db_task >> extract_task >> transform_task >> validate_task >> load_task

