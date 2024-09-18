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
import codecs
import re
import os

from scriptConvertTIff import reproject_tiff

# Ruta al archivo TIFF que se va a subir a MinIO
algorithm_output_tiff = './dags/repo/recursos/Orto_32629_1tif.tif'


def process_heatmap_data(**context):
    # Obtener el valor de 'type' de default_args a través del contexto
    task_type = context['dag'].default_args.get('type')

    # Realizar una operación condicional basada en el valor de 'type'
    if task_type == 'incendios':
        # Lógica específica para el heatmap de incendios
        print("Procesando datos para el heatmap de incendios.")
        # Modificaciones o lógica específica para incendios
        # input_data["ar_incendios"] = "historical_fires.csv"
        # input_data["url_search_fire"] = "https://pre.atcservices.cirpas.gal/rest/FireService/searchByIntersection"
        # input_data["url_fireperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/FireAlgorithm_FirePerimeterService/getByFire?id="

    elif task_type == 'aeronaves':
        # Lógica específica para el heatmap de aeronaves
        print("Procesando datos para el heatmap de aeronaves.")
        # Modificaciones o lógica específica para aeronaves
        # input_data["ar_aeronaves"] = "historical_aircraft.csv"
        # input_data["url_search_aircraft"] = "https://pre.atcservices.cirpas.gal/rest/AircraftService/searchByIntersection"
        # input_data["url_aircraftperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/AircraftAlgorithm_AircraftPerimeterService/getByAircraft?id="
    
    # El resto de tu código continúa aquí...

    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    from_user = str(message['message']['from_user'])
    input_data = json.loads(input_data_str)

   
    # input_data["temp_tiff_path"] = TIFF2
    input_data["dir_output"] = "/home/airflow/workspace/output"
    input_data["user"] = "usuario"
    input_data["password"] = "contraseña"

    # Aquí se ejecuta el algoritmo y deja de salida en el directorio 

    # Log para verificar que los datos de entrada son correctos
    print("Datos completos de entrada:")
    print(json.dumps(input_data, indent=4))

    # Subir el archivo TIFF a MinIO

    # cambiar_proyeccion_tiff(input_tiff=TIFF,output_tiff=TIFF2)
    # output_tiff = crear el directorio del output tiff con uuid 
    tiff_key = f"{uuid.uuid4()}.tiff"
    reproject_tiff(algorithm_output_tiff,"./dags/repo/recursos/" + tiff_key)
    # input_data["temp_tiff_path"] = output_tiff
   
    try:
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        bucket_name = 'temp'
        
        s3_client.upload_file("./dags/repo/recursos/" + tiff_key, bucket_name, tiff_key)
        tiff_url = f"https://minioapi.avincis.cuatrodigital.com/{bucket_name}/{tiff_key}"
        print(f"Archivo TIFF subido correctamente a MinIO. URL: {tiff_url}")

    except Exception as e:
        print(f"Error al subir el TIFF a MinIO: {str(e)}")
        return

    # Preparar la notificación para almacenar en la base de datos
    notification_db = {
        "to": from_user,
        "actions": [
            {
            "type": "notify",
            "data": {
                "message": "Datos del heatmap procesados correctamente"
            }
            },
            {
            "type": "paintTiff",
            "data": {
                "url": tiff_url
            }
            }
        ]
    }
    notification_json = json.dumps(notification_db, ensure_ascii=False)

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


# def cambiar_proyeccion_tiff(input_tiff, output_tiff):
#     # Abrir el archivo TIFF
#     dataset = gdal.Open(input_tiff, gdal.GA_Update)

#     if dataset is None:
#         raise FileNotFoundError(f"No se pudo abrir el archivo TIFF: {input_tiff}")

#     # Obtener la proyección actual
#     proyeccion = dataset.GetProjection()

#     # Crear un objeto SpatialReference
#     srs = osr.SpatialReference()

#     # Revisar si ya tiene proyección
#     if proyeccion:
#         print(f"Proyección actual: {proyeccion}")

#         # Si ya es EPSG:3857, no se cambia
#         srs.ImportFromWkt(proyeccion)
#         if srs.IsProjected() and srs.GetAttrValue("AUTHORITY", 1) == '3857':
#             print("El archivo ya tiene la proyección EPSG:3857.")
#         else:
#             # Cambiar proyección a EPSG:3857
#             print("Cambiando la proyección a EPSG:3857.")
#             srs.ImportFromEPSG(3857)
#             dataset.SetProjection(srs.ExportToWkt())
#     else:
#         # Si no tiene proyección, aplicar EPSG:3857
#         print("No tiene proyección, aplicando EPSG:3857.")
#         srs.ImportFromEPSG(3857)
#         dataset.SetProjection(srs.ExportToWkt())

#     # Guardar el archivo TIFF con la nueva proyección
#     gdal.Warp(output_tiff, dataset, dstSRS='EPSG:3857')

#     # Cerrar el dataset
#     dataset = None

#     print(f"Se ha guardado el archivo con la proyección EPSG:3857 en: {output_tiff}")


# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'type': 'incendios',
}

default_args_aero = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'type': 'aeronaves',
}


# Definición del DAG incendios
dag = DAG(
    'process_heatmap_incendios',
    default_args=default_args,
    description='DAG para procesar datos de heatmap-incendio, subir TIFF a MinIO, y enviar notificaciones',
    schedule_interval=None,
    catchup=False
)

# Definición del DAG aeronaves
dag_aero = DAG(
    'process_heatmap_aeronaves',
    default_args=default_args_aero,
    description='DAG para procesar datos de heatmap-aeronave, subir TIFF a MinIO, y enviar notificaciones',
    schedule_interval=None,
    catchup=False
)

# Tarea para el proceso de Heatmap de Aeronaves
process_heatmap_aeronaves_task = PythonOperator(
    task_id='process_heatmap_aeronaves',
    provide_context=True,
    python_callable=process_heatmap_data,
    dag=dag_aero,
)

# Tarea para el proceso de Heatmap de Incendios
process_heatmap_incendios_task = PythonOperator(
    task_id='process_heatmap_incendios',
    provide_context=True,
    python_callable=process_heatmap_data,
    dag=dag,
)

# Ejecución de la tarea en el DAG
process_heatmap_incendios_task 
process_heatmap_aeronaves_task
