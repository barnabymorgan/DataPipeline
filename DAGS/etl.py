from airflow.models import DAG
import os
from os.path import expanduser
from pathlib import Path
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

home = expanduser("~")
airflow_dir = os.path.join(home, 'airflow')
Path(f"{airflow_dir}/dags").mkdir(parents=True, exist_ok=True)

print(airflow_dir)

default_args = {
    'owner': 'Barney',
    'depends_on_past': False,
    'email': ['barnabygmorgan@outlook.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    #'end_date': datetime(202, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}

with DAG(dag_id='test_dag_new',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator
    test_task = BashOperator(
        task_id='write_date_file',
        bash_command='cd ~/Desktop && date >> ai_core.txt',
        dag=dag)
