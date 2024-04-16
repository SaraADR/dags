from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


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
    output = exec(open(spark_script_path).read())
    print(output)
    return output

with DAG(
    'spark_connection', 
    default_args=default_args, 
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["Spark"],
) as dag:

    # spark_task = PythonOperator(
    #     task_id='spark_id',
    #     python_callable=execute_spark_script,
    #     op_kwargs={'spark_script_path': '/opt/airflow/dags/repo/archivos/spark1.py'},
    #     dag=dag,
    # )

    task_elt_documento_pagar = SparkSubmitOperator(
    task_id='elt_documento_pagar_spark',
    conn_id='spark_id',
    application="/opt/airflow/dags/repo/archivos/spark1.py",
    application_args=[],#caso precise enviar dados da dag para o job airflow utilize esta propriedade
    total_executor_cores=2,
    executor_memory="30g",
    conf={},
    packages=""
    )