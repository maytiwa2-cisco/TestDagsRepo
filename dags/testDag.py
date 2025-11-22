from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello from Airflow!")

def print_goodbye():
    print("Goodbye from Airflow!")

with DAG(
    dag_id='simple_example_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    },
    description='A simple Airflow 3.0.2 DAG with no external dependencies',
    tags=['example', 'simple'],
) as dag:
    start = DummyOperator(task_id='start')

    hello = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello
    )

    bash_task = BashOperator(
        task_id='bash_echo',
        bash_command='echo "This is a Bash task!"'
    )

    goodbye = PythonOperator(
        task_id='say_goodbye',
        python_callable=print_goodbye
    )

    end = DummyOperator(task_id='end')

    start >> hello >> bash_task >> goodbye >> end
