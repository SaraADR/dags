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
    name='arrow-spark',
    dag=dag,
    conf={
        'spark.master': 'spark://spark-master-svc:7077',
        'spark.executor.memory': '2g',
    },
    verbose=False,
)

spark_task
