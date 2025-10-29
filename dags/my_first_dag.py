from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="my_first_dag",
    description="a simple tutorial DAG",
    schedule=None,
    start_date=datetime(2025,1,1),
    catchup=False
) as dag:
    task_one = BashOperator(
        task_id="first_task",
        bash_command="echo hello from the first task",
    )
    task_two = BashOperator(
        task_id="second_task",
        bash_command="echo hello from the second task",
    )
    task_one >> task_two