from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from minio import Minio

# Importamos el archivo que subiste para la configuración de MinIO
from create_bucket_minio import get_minio_client

# Definimos el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'delete_old_files_minio',
    default_args=default_args,
    description='DAG que borra archivos de MinIO con más de 24 horas',
    schedule_interval=timedelta(days=1),
)

def delete_old_files():
    client = get_minio_client()  # Usamos la función del archivo proporcionado

    bucket_name = 'temp-test'
    objects = client.list_objects(bucket_name)

    for obj in objects:
        # Calculamos la diferencia de tiempo entre ahora y la última modificación del archivo
        time_diff = datetime.now() - obj.last_modified.replace(tzinfo=None)

        # Si el archivo tiene más de 24 horas, lo borramos
        if time_diff > timedelta(minutes=1):
            client.remove_object(bucket_name, obj.object_name)
            print(f'Archivo {obj.object_name} borrado')

# Definimos la tarea que ejecutará la función anterior
delete_files_task = PythonOperator(
    task_id='delete_old_files',
    python_callable=delete_old_files,
    dag=dag,
)

delete_files_task
