from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Sadr",
    "depend_on_past": False,
    "start_date": datetime(2024, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def execute_spark_script(spark_script_path):
    exec(open(spark_script_path).read())

with DAG(
    'spark_connection', 
    default_args=default_args, 
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["Spark"],
) as dag:

    spark_task = PythonOperator(
        task_id='spark_id',
        python_callable=execute_spark_script,
        op_kwargs={'spark_script_path': '/opt/airflow/dags/repo/archivos/spark1.py'},
        dag=dag,
    )