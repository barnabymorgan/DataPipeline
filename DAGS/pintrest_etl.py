import os
from os.path import expanduser
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from pathlib import Path


"""
DAG to extract Pintrest data and copy to Apache Cassandra
"""

home = expanduser("~")
airflow_dir = os.path.join(home, 'airflow')
assert os.path.isdir(airflow_dir)

home = expanduser("~")
airflow_dir = os.path.join(home, 'airflow')
Path(f"{airflow_dir}/dags").mkdir(parents=True, exist_ok=True)


# Output name of extracted file. This be passed to each 
# DAG task so they know which file to process
output_name = datetime.now().strftime("%Y%m%d")
consumed_at = (datetime.strftime(datetime.today() 
                - timedelta(1) ,'%Y-%m-%d'))

# Run our DAG daily and ensures DAG run will kick off 
# once Airflow is started, as it will try to "catch up"
schedule_interval = '@once' 
start_date = days_ago(1)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id='elt_pintrest_pipeline',
    description ='Pintrest ELT from S3 to Cassandra',
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=['PintrestETL'],
) as dag:
    
    copy_to_cassandra = BashOperator(
        task_id = 'copy_to_cassandra',
        #bash_command = "python3 /Users/barnabymorgan/Desktop/DataPipeline/Consumer/spark_to_cass.py",
        bash_command = "python3 /Users/barnabymorgan/Desktop/DataPipeline/Consumer/af_test.py",
        dag = dag
    )
    copy_to_cassandra.doc_md = 'Copy S3 CSV file to Cassandra table'

copy_to_cassandra
