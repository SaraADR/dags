from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile


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
    first_40_values = message[:40]
    print(f"Received message: {first_40_values}")
    
    # Directorio temporal para descomprimir los archivos
    temp_unzip_path = "./dags/repo/temp/unzip"
    temp_zip_path = "./dags/repo/temp/zip"

    # Crear los directorios temporales si no existen
    os.makedirs(temp_unzip_path, exist_ok=True)
    os.makedirs(temp_zip_path, exist_ok=True)
    
    # Guardar el contenido del archivo zip en un archivo temporal
    zip_filename = os.path.join(temp_zip_path, 'temp_file.zip')
    with open(zip_filename, 'wb') as f:
        f.write(message['file_content'])

    # Descomprimir el archivo zip
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(temp_unzip_path)
    
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
    
    # Limpiar los directorios temporales después de procesar el archivo zip
    os.remove(zip_filename)
    for extracted_file in os.listdir(temp_unzip_path):
        os.remove(os.path.join(temp_unzip_path, extracted_file))


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
