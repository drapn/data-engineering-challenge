from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.etl import extract_load, transform, report

with DAG('nyc_taxi_2022', start_date=datetime(2022, 1, 1), schedule=None,
         catchup=False, max_active_runs=1,
         default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)},
         tags=['taxi', 'etl']) as dag:
    raw = PythonOperator(task_id='extract_load_raw', python_callable=extract_load)
    trusted = PythonOperator(task_id='transform', python_callable=transform)
    answers = PythonOperator(task_id='quality_and_answers', python_callable=report)
    raw >> trusted >> answers
