from datetime import datetime, timedelta
import tempfile
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
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.orm import sessionmaker
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.providers.ssh.hooks.ssh import SSHHook
from scriptConvertTIff import reproject_tiff



def process_heatmap_data(**context):

    # Obtener el valor de 'type' de default_args a través del contexto
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)
    task_type = message['message']['job']

    isIncendio = "FALSE"
    arincendios = ''
    if task_type == 'heatmap-incendios':
        # Lógica específica para el heatmap de incendios
        print("Procesando datos para el heatmap de incendios.")
  
        # Modificaciones o lógica específica para incendios
        arincendios = "historical_fires.csv"
        isIncendio = "TRUE"
        # input_data["url_search_fire"] = "https://pre.atcservices.cirpas.gal/rest/FireService/searchByIntersection"
        # input_data["url_fireperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/FireAlgorithm_FirePerimeterService/getByFire?id="

    elif task_type == 'heatmap-aeronaves':
        # Lógica específica para el heatmap de aeronaves
        print("Procesando datos para el heatmap de aeronaves.")
        
        # Modificaciones o lógica específica para aeronaves
        arincendios = "historical_aircraft.csv"
        isIncendio = "FALSE"
        # input_data["url_search_aircraft"] = "https://pre.atcservices.cirpas.gal/rest/AircraftService/searchByIntersection"
        # input_data["url_aircraftperimeter_service"] = "https://pre.atcservices.cirpas.gal/rest/AircraftAlgorithm_AircraftPerimeterService/getByAircraft?id="
    

    params = {
        "directorio_output":  '/share_data/output/' + str(task_type) + '_' + str(message['message']['id']),
        "incendios" : isIncendio,
        "ar_incendios": None,
        "comunidadAutonomaId":  input_data.get('comunidadId', None),
        "lowSearchDate" : input_data.get('lowSearchDate', None),
        "highSearchDate" : input_data.get('highSearchDate', None),
        "sigma" :  input_data.get('sigma', None),
        "codigo" : input_data.get('codigo', None)
    }

    # Generar el archivo JSON dinámicamente con los valores obtenidos
    json_file_path = create_json(params)


    #Guardar el json en la carpeta correspondiente
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")


            print(f"Cambiando al directorio de lanzamiento y ejecutando limpieza de voluemnes")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/Algoritmo_mapas_calor/algoritmo-mapas-de-calor-objetivo-1-master/launch && docker-compose down --volumes')
            
            output = stdout.read().decode()
            error_output = stderr.read().decode()

            print("Salida de docker volumes:")
            print(output)



            remote_directory = '/home/admin3/Algoritmo_mapas_calor/algoritmo-mapas-de-calor-objetivo-1-master/input'
            remote_file_name =  str(task_type) + '_' + str(message['message']['id']) + '.json'
            remote_file_path = os.path.join(remote_directory, remote_file_name)

            # Guardar los cambios de nuevo en el archivo
            with sftp.file(remote_file_path, 'w') as remote_file:
                json.dump(json_file_path, remote_file, indent=4)  
                print(f"Archivo {remote_file_name} actualizado en {remote_directory}")


            config_path = '/share_data/input/' +  str(task_type) + '_' + str(message['message']['id']) + '.json'
            command = f'cd /home/admin3/Algoritmo_mapas_calor/algoritmo-mapas-de-calor-objetivo-1-master/launch && CONFIGURATION_PATH={config_path} docker-compose -f compose.yaml up --build'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            output = stdout.read().decode()
            error_output = stderr.read().decode()

            print("Salida de docker:")
            print(output)

            sftp.close()
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")













    # Subir el archivo TIFF a MinIO

    # cambiar_proyeccion_tiff(input_tiff=TIFF,output_tiff=TIFF2)
    # output_tiff = crear el directorio del output tiff con uuid 









    # tiff_key = f"{uuid.uuid4()}.tiff"
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     temp_dir_file = os.path.join(temp_dir, tiff_key)

    #     reproject_tiff(algorithm_output_tiff, temp_dir_file)
    #     # input_data["temp_tiff_path"] = output_tiff
    
    #     try:
    #         connection = BaseHook.get_connection('minio_conn')
    #         extra = json.loads(connection.extra)
    #         s3_client = boto3.client(
    #             's3',
    #             endpoint_url=extra['endpoint_url'],
    #             aws_access_key_id=extra['aws_access_key_id'],
    #             aws_secret_access_key=extra['aws_secret_access_key'],
    #             config=Config(signature_version='s3v4')
    #         )
    #         bucket_name = 'temp'
    #         s3_client.upload_file(temp_dir_file, bucket_name, tiff_key)
    #         tiff_url = f"https://minioapi.avincis.cuatrodigital.com/{bucket_name}/{tiff_key}"
    #         print(f"Archivo TIFF subido correctamente a MinIO. URL: {tiff_url}")

    #     except Exception as e:
    #         print(f"Error al subir el TIFF a MinIO: {str(e)}")
    #         return

    #     # Preparar la notificación para almacenar en la base de datos
    #     notification_db = {
    #         "to": from_user,
    #         "actions": [
    #             {
    #             "type": "notify",
    #             "data": {
    #                 "message": "Datos del heatmap procesados correctamente"
    #             }
    #             },
    #             {
    #             "type": "paintTiff",
    #             "data": {
    #                 "url": tiff_url
    #             }
    #             }
    #         ]
    #     }
    #     notification_json = json.dumps(notification_db, ensure_ascii=False)

    #     # Insertar la notificación en la base de datos PostgreSQL
    #     try:
    #         connection = BaseHook.get_connection('biobd')
    #         pg_hook = PostgresOperator(
    #             task_id='send_notification',
    #             postgres_conn_id='biobd',
    #             sql=f"""
    #             INSERT INTO public.notifications (destination, data)
    #             VALUES ('ignis', '{notification_json}');
    #             """
    #         )
    #         pg_hook.execute(context)
    #         print("Notificación almacenada correctamente en la base de datos.")

    #     except Exception as e:
    #         print(f"Error al almacenar la notificación en la base de datos: {str(e)}")

def change_state_job(**context):
    message = context['dag_run'].conf
    job_id = message['message']['id']
    print(f"jobid {job_id}" )

    try:
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Update job status to 'FINISHED'
        metadata = MetaData(bind=engine)
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado del estado del job: {str(e)}")

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



def create_json(params):
    input_data = {
        "directorio_alg": params.get("directorio_alg", "."),
        "directorio_output": params.get("directorio_output"),
        "incendios": params.get("incendios", "FALSE"),
        "ar_incendios": params.get("ar_incendios", None),
        "url1": params.get("url1", None),
        "url2": params.get("url2", None),
        "url3": params.get("url3", None),
        "url4": params.get("url4", None),
        "user": params.get("user", "ITMATI.DES"),
        "password": params.get("password", "Cui_1234"),
        "minlat": params.get("minlat", None),
        "maxlat": params.get("maxlat", None),
        "minlon": params.get("minlon", None),
        "maxlon": params.get("maxlon", None),
        "comunidadAutonomaId": params.get("comunidadAutonomaId", None),
        "lowSearchDate": params.get("lowSearchDate", None),
        "highSearchDate": params.get("highSearchDate", None),
        "navstates": params.get("navstates", None),
        "title": params.get("title", None),
        "customerIds": params.get("customerIds", "XUNTAGALICIA_MEDIORURAL"),
        "res": params.get("res", None),
        "codigo": params.get("codigo", None),
        "sigma": params.get("sigma", None)
    }


    return input_data




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
    'process_heatmap',
    default_args=default_args,
    description='DAG para procesar datos de heatmap-incendio, subir TIFF a MinIO, y enviar notificaciones',
    schedule_interval=None,
    catchup=False
)

# Tarea para el proceso de Heatmap de Incendios
process_heatmap_task = PythonOperator(
    task_id='process_heatmap_incendios',
    provide_context=True,
    python_callable=process_heatmap_data,
    dag=dag,
)

#Cambia estado de job
change_state_task = PythonOperator(
    task_id='change_state_job',
    python_callable=change_state_job,
    provide_context=True,
    dag=dag,
)

# Ejecución de la tarea en el DAG
process_heatmap_task >> change_state_task
