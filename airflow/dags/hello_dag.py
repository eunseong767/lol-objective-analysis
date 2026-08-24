from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("첫 DAG 실행 성공")


def say_bye():
    print("두 번째 작업 완료")


with DAG(
    dag_id="hello_lol",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    task1 = PythonOperator(
        task_id="hello",
        python_callable=say_hello,
    )

    task2 = PythonOperator(
        task_id="bye",
        python_callable=say_bye,
    )

    task1 >> task2