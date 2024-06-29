from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'example_spark_dag',
    default_args=default_args,
    description='A simple Spark DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 26),
    catchup=False,
)

spark_task = SparkSubmitOperator(
    task_id='spark_submit_job',
    application='/opt/airflow/dags/repo/spark/sparktest.py',
    conn_id='spark_default',
    executor_memory='2G',   # Increase executor memory
    driver_memory='2G',     # Increase driver memory
    total_executor_cores=4, # Increase total executor cores
    executor_cores=2,       # Increase executor cores per executor
    name='arrow-spark',
    queue='default',
    dag=dag,
    verbose=False,
)

spark_task
