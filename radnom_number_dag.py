from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import random

def generate_random_number(**kwargs):
    number = random.randint(1, 100)
    print(f"Generated number: {number}")
    return number

def check_multiple(**kwargs):
   ti = kwargs["ti"]
   number = ti.xcom_pull(task_ids="task_generate_number")
   if number % 3 == 0 and number % 5 == 0:
       result = "multiple of both"
   elif number % 3 == 0:
      result = "multiple  of 3"
   elif number % 5 == 0:
      result = "multiple of 5"
   else:
      result = "not a multiple of 3 or 5"

   print(f"Check result: {result}")
   return result

with DAG(
    dag_id="random_number_manipulation", 
    schedule_interval="@daily", 
    start_date=days_ago(1), 
    catchup=False,
    tags=["classwork"]
) as dag:

   task_generate_number = PythonOperator(
       task_id="task_generate_number",
       python_callable=generate_random_number,
       provide_context=True
   ) 

   task_check_multiple = PythonOperator(
       task_id="task_check_multiple",
       python_callable=check_multiple, 
       provide_context=True
   )

   task_print_message = BashOperator(
       task_id="task_print_message", 
       bash_command='echo "{{ ti.xcom_pull(task_ids=\"task_check_multiple\") }}"'
   )

   task_generate_number >> task_check_multiple >> task_print_message 
