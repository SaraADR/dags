from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import uuid
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Ruta al archivo TIFF que se va a subir a MinIO
TIFF = './dags/repo/recursos/f496d404-85d9-4c66-9b16-1e5fd9da85b9.tif'


def process_heatmap_data(**context):

    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    from_user = message['message']['from_user']
    input_data = json.loads(input_data_str)

    
    input_data ["temp_tiff_path"] = TIFF
    input_data ["dir_output"] = "/home/airflow/workspace/output"
    input_data ["ar_incendios"] = "historical_fires.csv"
    input_data ["url_search_fire"] = "https://pre.atcservices.cirpas.gal/rest/FireService/searchByIntersection"
    input_data ["url_fireperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/FireAlgorithm_FirePerimeterService/getByFire?id="
    input_data ["user"] = "usuario"
    input_data ["password"] = "contraseña"


    # Log para verificar que los datos de entrada son correctos
    print("Datos completos de entrada para heatmap-incendio:")
    print(json.dumps(input_data, indent=4))

    # Subir el archivo TIFF a MinIO
    try:
        # Conexión a MinIO utilizando las credenciales almacenadas en Airflow
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        # Generar un nombre único (uuid) para el archivo en MinIO
        bucket_name = 'temp'
        tiff_key = f"{uuid.uuid4()}.tiff"

        # Subir el archivo TIFF al bucket de MinIO
        s3_client.upload_file(input_data['temp_tiff_path'], bucket_name, tiff_key)
        tiff_url = f"{"https://minioapi.avincis.cuatrodigital.com"}/{bucket_name}/{tiff_key}"
        print(f"Archivo TIFF subido correctamente a MinIO. URL: {tiff_url}")
        
        #Excepción si hay un error al subir a minio el archivo tiff
    except Exception as e:
        print(f"Error al subir el TIFF a MinIO: {str(e)}")
        return
    
    # Preparar la notificación para almacenar en la base de datos
    notification_db = {
        # "type": "heat_map",
        "message": "Heatmap data processed and TIFF uploaded",
        # "destination": "ignis",
        # "input_data": input_data
        "to": from_user,
        'urlTiff': tiff_url
    }

    # Convertir la notificación a formato JSON
    notification_json = json.dumps(notification_db)

    # Insertar la notificación en la base de datos PostgreSQL
    try:
        connection = BaseHook.get_connection('biobd')
        pg_hook = PostgresOperator(
            task_id='send_notification',
            postgres_conn_id='biobd',
            sql=f"""
            INSERT INTO public.notifications (destination, data)
            VALUES ('ignis', '{notification_json}');
            """
        )
        pg_hook.execute(context)
        print("Notificación almacenada correctamente en la base de datos.")
        
    except Exception as e:
        print(f"Error al almacenar la notificación en la base de datos: {str(e)}")

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definición del DAG
dag = DAG(
    'heatmap_incendio_process',
    default_args=default_args,
    description='DAG para procesar datos de heatmap-incendio, subir TIFF a MinIO, y enviar notificaciones',
    schedule_interval=None,
    catchup=False
)

# Definición de la tarea principal
process_heatmap_task = PythonOperator(
    task_id='process_heatmap_data',
    python_callable=process_heatmap_data,
    provide_context=True,
    dag=dag,
)

# Ejecución de la tarea en el DAG
process_heatmap_task
