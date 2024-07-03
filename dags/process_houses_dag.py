from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Définir les chemins
script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'process_houses.py')

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_houses_dag',
    default_args=default_args,
    description='Pipeline de traitement des données immobilières',
    schedule_interval=timedelta(minutes=1),
)

# Définir la tâche
def process_houses():
    os.system(f'python {script_path}')

process_houses_task = PythonOperator(
    task_id='process_houses_task',
    python_callable=process_houses,
    dag=dag,
)

process_houses_task
