from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_current_time():
    """Функция для вывода текущего времени"""
    current_time = datetime.now()
    print(f"Текущее время: {current_time}")
    return str(current_time)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'test_12',
    default_args=default_args,
    description='Простой DAG для вывода текущего времени (test 12)',
    schedule='0 10 * * *',  # Каждый день в 10:00
    catchup=False,
    tags=['test', 'time'],
) as dag:


    print_time_task = PythonOperator(
        task_id='print_time',
        python_callable=print_current_time,
    )

    print_time_task