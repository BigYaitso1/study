from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello():
    print("STAGING TEST - version 2")


with DAG(
    dag_id="hello_dag",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )