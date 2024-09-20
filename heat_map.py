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
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from scriptConvertTIff import reproject_tiff

# Ruta al archivo TIFF que se va a subir a MinIO
algorithm_output_tiff = './dags/repo/recursos/Orto_32629_1tif.tif'


def process_heatmap_data(**context):
    # Obtener el valor de 'type' de default_args a través del contexto
    task_type = context['dag'].default_args.get('type')

    # Realizar una operación condicional basada en el valor de 'type'
    if task_type == 'incendios':
        print("Procesando datos para el heatmap de incendios.")
    elif task_type == 'aeronaves':
        print("Procesando datos para el heatmap de aeronaves.")

    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    from_user = str(message['message']['from_user'])
    job_id = message['message'].get('job_id')  # Asegúrate de obtener el ID del trabajo

    input_data = json.loads(input_data_str)
    input_data["dir_output"] = "/home/airflow/workspace/output"
    input_data["user"] = "usuario"
    input_data["password"] = "contraseña"

    print("Datos completos de entrada:")
    print(json.dumps(input_data, indent=4))

    # Subir el archivo TIFF a MinIO
    tiff_key = f"{uuid.uuid4()}.tiff"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_file = os.path.join(temp_dir, tiff_key)
        reproject_tiff(algorithm_output_tiff, temp_dir_file)
    
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
            s3_client.upload_file(temp_dir_file, bucket_name, tiff_key)
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

        try:
            connection = BaseHook.get_connection('biobd')
            engine = create_engine(connection.get_uri())  # Crear el engine usando SQLAlchemy
            Session = sessionmaker(bind=engine)
            session = Session()

            # Insertar la notificación en la base de datos
            session.execute(
                f"INSERT INTO public.notifications (destination, data) VALUES ('ignis', '{notification_json}');"
            )
            session.commit()
            print("Notificación almacenada correctamente en la base de datos.")

            # **Actualización del estado del job a 'FINISHED'**
            metadata = MetaData(bind=engine)
            jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
            update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED')
            session.execute(update_stmt)
            session.commit()
            print(f"Job ID {job_id} status updated to FINISHED")

        except Exception as e:
            session.rollback()
            print(f"Error al almacenar la notificación o actualizar el estado del job en la base de datos: {str(e)}")

        finally:
            session.close()


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
