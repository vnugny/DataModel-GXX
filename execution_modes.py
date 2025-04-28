from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime
from great_expectations.data_context import DataContext

def validate():
    context = DataContext()
    context.run_validation_operator("action_list_operator", assets_to_validate=[])

dag = DAG("gx_validation_dag", start_date=datetime(2023, 1, 1), schedule_interval="@daily")

task = PythonOperator(
    task_id="run_gx_validation",
    python_callable=validate,
    dag=dag
)