import io
import tempfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
import ast

# Configuración de MinIO
# minio_client = Minio(
#     "storage-minio:9000",
#     access_key="dMxwH6VwKyUrjeX38J3y",
#     secret_key="vgtcFgEp7zeWftjSh7pAnZsYCKn2DIkAoRfQlvzD",
#     secure=False
# )


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

    # Crear un directorio temporal utilizando el módulo tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_unzip_path = os.path.join(temp_dir, 'unzip')
        temp_zip_path = os.path.join(temp_dir, 'zip')

        # Crear los subdirectorios temporales
        os.makedirs(temp_unzip_path, exist_ok=True)
        os.makedirs(temp_zip_path, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(file_content)) as zip_file:
        # Obtener la lista de archivos dentro del ZIP
            file_list = zip_file.namelist()
            print("Archivos en el ZIP:", file_list)

            for file_name in file_list:
                with zip_file.open(file_name) as file:
                    content = file.read()
                    print(f"Contenido del archivo {file_name}: {content[:10]}...")  


        print(f"Se han creado los temporales")
        # # Subir archivos descomprimidos a MinIO
        # for extracted_file in os.listdir(temp_unzip_path):
        #     extracted_file_path = os.path.join(temp_unzip_path, extracted_file)
        #     if extracted_file.endswith(".pdf") or extracted_file.endswith(".docx"):
        #         with open(extracted_file_path, 'rb') as file_data:
        #             file_stat = os.stat(extracted_file_path)
        #             minio_client.put_object(
        #                 "avincis-test",
        #                 extracted_file,
        #                 file_data,
        #                 file_stat.st_size
        #             )
        
        # Los directorios temporales se limpiarán automáticamente al salir del bloque 'with'


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
