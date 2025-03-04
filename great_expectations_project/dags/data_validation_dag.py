from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import subprocess

# Define paths to JSON configurations
DATA_SOURCE_PATH = "/opt/airflow/dags/configs/data_source_config.json"
EXPECTATIONS_PATH = "/opt/airflow/dags/configs/expectations_config.json"

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2024, 3, 1),
    "retries": 1,
}

# Define the DAG
dag = DAG(
    dag_id="data_validation_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

def run_validation():
    """Executes the validation script using the configured JSON files."""
    subprocess.run(["python", "/opt/airflow/dags/scripts/validation.py", DATA_SOURCE_PATH, EXPECTATIONS_PATH])

# Task: Run Data Validation
validate_task = PythonOperator(
    task_id="run_data_validation",
    python_callable=run_validation,
    dag=dag,
)

# Set task dependencies
validate_task
