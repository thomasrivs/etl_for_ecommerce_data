from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.extract import get_data
from etl.load import load_data_into_gcs, transform_data_to_csv

with DAG("my_dag", # Dag id
  start_date=datetime(2023, 1 ,1), # start date, the 1st of January 2023
  schedule='@daily', # Cron expression, here @daily means once every day.
  catchup=False
):
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable= get_data
    )

    load_data = PythonOperator(
        task_id="load",
        python_callable= load_data_into_gcs
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable= transform_data_to_csv
    )

    extract_data >> load_data >> transform