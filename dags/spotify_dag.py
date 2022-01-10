import sys
from datetime import timedelta
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
sys.path.insert(1, '/mnt/c/Users/Denzel/Documents/spotify_etl')
from etl import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 9),
    'email': ["denzelbrown@me.com"],
    'email-on-failure': True,
    'email-on-retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description="DAG for spotify etl process",
    schedule_interval=timedelta(days=1),
)


def just_a_function():
    print("I'm going to show you something")


run_etl = PythonOperator(
    task_id="spotify_etl",
    python_callable=run_spotify_etl,
    dag=dag,
)

run_etl
