import io
import tempfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
import ast
from minio import Minio
from minio.error import S3Error
from airflow.hooks.base_hook import BaseHook

def process_kafka_message(**context):
    # Extraer el mensaje del contexto de Airflow
    message = context['dag_run'].conf
    
    # Parse the message content
    message_dict = ast.literal_eval(message['message'])

    # Verificar que la clave 'file_content' esté presente en el mensaje
    if message:
        file_content = message['message']
        # Mostrar los primeros 40 caracteres del contenido del archivo
        first_40_values = file_content[:40]
        print(f"Received file content (first 40 bytes): {first_40_values}")
    else:
        raise KeyError("The key 'file_content' was not found in the message.")

    message_dict = ast.literal_eval(message['message'])
    # Crear un directorio temporal utilizando el módulo tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_unzip_path = os.path.join(temp_dir, 'unzip')
        temp_zip_path = os.path.join(temp_dir, 'zip')

        # Crear los subdirectorios temporales
        os.makedirs(temp_unzip_path, exist_ok=True)
        os.makedirs(temp_zip_path, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(message_dict)) as zip_file:
        # Obtener la lista de archivos dentro del ZIP
            file_list = zip_file.namelist()
            print("Archivos en el ZIP:", file_list)

            for file_name in file_list:
                with zip_file.open(file_name) as file:
                    content = file.read()
                    print(f"Contenido del archivo {file_name}: {content[:10]}...")  
                    save_to_minio(file_name, content)

        print(f"Se han creado los temporales")


def save_to_minio(file_name, content):
    # Obtener la conexión de MinIO desde Airflow
    connection = BaseHook.get_connection('minio_conn')
    minio_client = Minio(
        f'{connection.host}:{connection.port}',
        access_key=connection.login,
        secret_key=connection.password,
        secure=False  # Cambiar a True si estás usando HTTPS
    )

    bucket_name = 'avincis-test'  # Reemplaza esto con tu bucket de MinIO

    # Crear el bucket si no existe
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Subir el archivo a MinIO
    minio_client.put_object(
        bucket_name,
        file_name,
        io.BytesIO(content),
        length=len(content),
    )
    print(f'{file_name} subido correctamente a MinIO.')




# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'save_documents_to_minio',
    default_args=default_args,
    description='A simple DAG to save documents to MinIO',
    schedule_interval=timedelta(days=1),
)

# Definir la tarea
save_task = PythonOperator(
    task_id='save_to_minio_task',
    provide_context=True,
    python_callable=process_kafka_message,
    dag=dag,
)

# Establecer las dependencias del DAG
save_task
