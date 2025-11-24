import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import sqlite3
import os
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries":1,
}

with DAG(
   dag_id="weather_etl_project",
   start_date=datetime(2025,11,19),
   schedule_interval="@daily",
   catchup=False,
   default_args=default_args,
   tags=["weather", "ETL"]
) as dag:

   data_dir = os.path.expanduser("~/airflow/data")
   os.makedirs(data_dir, exist_ok=True)
   db_path = os.path.join(data_dir, "weather.db")

   def extract( **context):
       os.environ["KAGGLE_CONFIG_DIR"] = os.path.expanduser("~/.kaggle")
       api = KaggleApi()
       api.authenticate()

       dataset = "muthuj7/weather-dataset"
       api.dataset_download_files(dataset, path=data_dir, unzip=False)

       zip_files = [f for f in os.listdir(data_dir) if f.endswith(".zip")]
       if not zip_files:
           raise FileNotFoundError("Zip file not found")

       zip_path = os.path.join(data_dir, zip_files[0])

       with zipfile.ZipFile(zip_path, "r") as zip_ref:
           zip_ref.extractall(data_dir)

       csv_path = os.path.join(data_dir, "weatherHistory.csv")
       if not os.path.exists(csv_path):
           raise FileNotFoundError("CSV file not found")

       context["ti"].xcom_push(key="csv_path", value=csv_path)

   extract_task = PythonOperator(task_id="extract", python_callable=extract)

   def categorize_wind(speed_kmh):
       speed_ms = speed_kmh / 3.6
       if speed_ms <= 1.5:
          return "Calm"
       elif speed_ms <= 3.3:
          return "Light Air"
       elif speed_ms <= 5.4:
          return "Light Breeze"
       elif speed_ms <= 7.9:
          return "Gentle Breeze"
       elif speed_ms <= 10.7:
          return "Moderate Breeze"
       elif speed_ms <= 13.8:
          return "Fresh Breeze"
       elif speed_ms <= 17.1:
          return "Strong Breeze"
       elif speed_ms <= 20.7:
          return "Near Gale"
       elif speed_ms <= 24.4:
          return "Gale"
       elif speed_ms <= 28.4:
          return "Strong Gale"
       elif speed_ms <= 32.6:
          return "Storm"
       else:
          return "Violent Storm"

   def transform(**context):
       csv_path = context["ti"].xcom_pull(key="csv_path", task_ids="extract") 
       df = pd.read_csv(csv_path)

       df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], errors="coerce", utc=True)
       df.drop_duplicates(inplace=True)
       df.dropna(subset=["Temperature (C)","Apparent Temperature (C)",  "Humidity", "Wind Speed (km/h)"], inplace=True)
       df.dropna(subset=["Formatted Date"], inplace=True)

       df.set_index("Formatted Date", inplace=True)
       df.index = pd.DatetimeIndex(df.index)

       daily_avg = df.resample("D").agg({
           "Temperature (C)": "mean",
           "Humidity": "mean",
           "Wind Speed (km/h)": "mean",
       }).rename(columns={
           "Temperature (C)" : "avg_temperature_c",
           "Humidity": "avg_humidity", 
           "Wind Speed (km/h)": "avg_wind_speed_kmh"
       })

       daily = df.copy()
       daily = daily.join(daily_avg, how="left")
       daily.dropna(inplace=True)
       daily["wind_strength"] = daily["avg_wind_speed_kmh"].fillna(0).apply(categorize_wind)

       daily.reset_index(inplace=True)
       daily.rename(columns={
           "Formatted Date": "formatted_date",
           "Precip Type": "precip type",
           "Temperature (C)": "temperature_c",
           "Apparent Temperature (C)": "apparent_temperature_c",
           "Humidity": "humidity",
           "Wind Speed (km/h)": "wind_speed_kmh",
           "Visibility (km)": "visibility_km",
           "Pressure (millibars)": "pressure_millibars"
       }, inplace=True)
       daily.insert(0, "id", range(1, len(daily)+1))

       daily = daily[["id", "formatted_date", "precip type", "temperature_c", "apparent_temperature_c", "humidity", "wind_speed_kmh", "visibility_km", "pressure_millibars", "wind_strength", "avg_temperature_c", "avg_humidity", "avg_wind_speed_kmh"]]

       daily_path = os.path.join(data_dir, "daily_weather.csv")
       daily.to_csv(daily_path, index=False)

       df["month"] = df.index.to_period("M").to_timestamp()
       monthly = df.groupby("month").agg({
           "Temperature (C)": "mean",
           "Apparent Temperature (C)": "mean",
           "Humidity": "mean",
           "Visibility (km)": "mean",
           "Pressure (millibars)": "mean",
           "Precip Type": lambda x: x.mode()[0] if not x.mode().empty else None
       }).rename(columns={
           "Temperature (C)": "avg_temperature_c",
           "Apparent Temperature (C)": "avg_apparent_temperature_c",
           "Humidity": "avg_humidity",
           "Visibility (km)": "avg_visibility_km",
           "Pressure (millibars)": "avg_pressure_millibars",
           "Precip Type": "mode_precip_type"
       }).reset_index()
       monthly.insert(0, "id", range(1, len(monthly)+1))

       monthly_path = os.path.join(data_dir, "monthly_weather.csv")
       monthly.to_csv(monthly_path, index=False)

       context["ti"].xcom_push(key="daily_csv", value=daily_path)
       context["ti"].xcom_push(key="monthly_csv", value=monthly_path)

   transform_task = PythonOperator(task_id="transform", python_callable=transform)

   def validate(**context):
       daily_path = context["ti"].xcom_pull(key="daily_csv", task_ids="transform")
       monthly_path = context["ti"].xcom_pull(key="monthly_csv", task_ids="transform")

       daily = pd.read_csv(daily_path)
       monthly = pd.read_csv(monthly_path)

       if daily[["avg_temperature_c","avg_humidity","avg_wind_speed_kmh"]].isnull().any().any():
           raise ValueError("Missing values in daily data!")
       if not daily["avg_temperature_c"].between(-50,50).all():
           raise ValueError("Temperature out of range!")
       if not daily["avg_humidity"].between(0,1).all():
           raise ValueError("Humidity out of range!")
       if (daily["avg_wind_speed_kmh"] < 0).any():
           raise ValueError("Negative wind speed!")

       if monthly[["avg_temperature_c", "avg_apparent_temperature_c", "avg_humidity"]].isnull().any().any():
           raise ValueError("Missing values in monthly data!")
       if not monthly["avg_temperature_c"].between(-50,50).all():
           raise ValueError("Temperature out of range!")
       if not monthly["avg_apparent_temperature_c"].between(-50,50).all():
           raise ValueError("Monthly apparent temperature out of range!")
       if not monthly["avg_humidity"].between(0,1).all():
           raise ValueError("Humidity out of range!")

       temp_outliers = daily[~daily["avg_temperature_c"].between(-30, 45)]
       if not temp_outliers.empty:
            print(f"Temperature outlier detected:\n{temp_outliers[['formatted_date','avg_temperature_c']]}")
       humidity_outliers = daily[~daily["avg_humidity"].between(0,1)]
       if not humidity_outliers.empty:
            print(f"Humidity outlier detected:\n{humidity_outliers[['formatted_date', 'avg_humidity']]}")
       wind_outliers = daily[daily["avg_wind_speed_kmh"] > 150]
       if not wind_outliers.empty:
            print(f"Wind speed outliers detected:\n{wind_outliers[['formatted_date', 'avg_wind_speed_kmh']]}")

       monthly_temp_outliers = monthly[~monthly["avg_temperature_c"].between(-30,45)]
       if not monthly_temp_outliers.empty:
            print(f"Monthly temperature outliers detected:\n{monthly_temp_outliers[['month', 'avg_temperature_c']]}")
       monthly_apparent_temp_outliers = monthly[~monthly["avg_apparent_temperature_c"].between(-30,45)]
       if not monthly_apparent_temp_outliers.empty:
            print(f"Monthly apparent temperature outliers detected:\n{monthly_apparent_temp_outliers[['month', 'avg_apparent_temperature_c']]}")
       monthly_humidity_outliers = monthly[~monthly["avg_humidity"].between(0,1)]
       if not monthly_humidity_outliers.empty:
            print(f"Monthly humidity outliers detected:\n{monthly_humidity_outliers[['month', 'avg_humidity']]}")

   validate_task = PythonOperator(task_id="validate", python_callable=validate, trigger_rule=TriggerRule.ALL_SUCCESS)

   def load(**context):
       daily_path = context["ti"].xcom_pull(key="daily_csv", task_ids="transform")
       monthly_path = context["ti"].xcom_pull(key="monthly_csv", task_ids="transform")

       conn = sqlite3.connect(db_path)
       pd.read_csv(daily_path).to_sql("daily_weather", conn, if_exists="replace", index=False)
       pd.read_csv(monthly_path).to_sql("monthly_weather", conn, if_exists="replace", index=False)
       conn.close()

   load_task = PythonOperator(task_id="load", python_callable=load)

   extract_task >> transform_task >> validate_task >> load_task
