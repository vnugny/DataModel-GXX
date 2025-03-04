from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import datetime
import subprocess

def validate_data():
    subprocess.run(["python", "run_validation.py"])

dag = DAG(
    dag_id="data_quality_pipeline",
    start_date=datetime.datetime(2024, 3, 1),
    schedule_interval="@daily",
)

validate_task = PythonOperator(
    task_id="run_data_validation",
    python_callable=validate_data,
    dag=dag,
)
