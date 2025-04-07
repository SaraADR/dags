import boto3
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import TaskInstance
from airflow.utils.db import provide_session

# Función que imprime un mensaje cuando un DAG termina
@provide_session
def print_message(session, **kwargs):
    dag_name = kwargs['dag_run'].conf.get('dag_name', 'Desconocido')
    print(f"Ha finalizado este DAG: {dag_name}")

    ti = task_instances = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id == dag_name)
        .limit(10)
        .all()
    )

    for ti in task_instances:
        print(f"📌 Log encontrado: DAG={ti.dag_id}, Run ID={ti.run_id}, Task={ti.task_id}, Intento={ti.try_number}")


    ti = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_name).order_by(TaskInstance.execution_date.desc()).limit(1).first()

    if not ti:
        log_path = "No se encontró una ejecución válida"
    else:
        log_path = f"/opt/airflow/logs/dag_id={dag_name}/run_id={ti.run_id}/task_id={ti.task_id}/attempt={ti.try_number}.log"

    print(f"🔍 Se va a buscar este log: {log_path}")
    return log_path


def upload_to_minio(**kwargs):
    log_path = kwargs['ti'].xcom_pull(task_ids='find_log')
    
    if not log_path or log_path.startswith("No se encontró"):
        print("🚨 No se puede subir el log a MinIO, no existe un archivo válido.")
        return
    
    # Leer el contenido del log
    with open(log_path, "r") as log_file:
        log_data = log_file.read()

    print(log_data)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 7),
    'retries': 1,
}

dag = DAG(
    'monitor_dags',
    default_args=default_args,
    description='Monitorea otros DAGs, busca sus logs y los sube a MinIO',
    schedule_interval=None,
)

print_msg_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)

upload_to_minio_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    provide_context=True,
    dag=dag,
)

print_msg_task >> upload_to_minio_task
