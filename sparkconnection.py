from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 17),
    'retries': 1
}

dag = DAG('spark_word_count', default_args=default_args, schedule_interval=None)

# Define la tarea que ejecutará el script de Spark
spark_task = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit /opt/airflow/dags/repo/spark/sparktest.py',
    dag=dag
)


spark_task


