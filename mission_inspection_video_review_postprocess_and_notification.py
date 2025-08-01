import base64
import io
from airflow.operators.python import PythonOperator
import json
import uuid
from airflow import DAG
from sqlalchemy import create_engine, Table, MetaData, text
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker
import boto3
from botocore.client import Config
import datetime
from dag_utils import get_db_session, get_minio_client


def process_element(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']


    # Convertir la cadena de input_data en un diccionario
    input_data = json.loads(input_data_str)
    uuid_key = uuid.uuid4()
    #Subir a minIO el recurso  thumbnail
    if(input_data['thumbnail'] is not None):

        try:
            s3_client = get_minio_client()

            bucket_name = 'missions'  
            png_key = str(uuid_key) + '/' + 'vegetation_detection_thumbnail' + '.png'
            decoded_bytes = base64.b64decode(input_data['thumbnail'].split(",")[1])

            # Subir el archivo a MinIO
            s3_client.put_object(
                Bucket=bucket_name,
                Key=png_key,
                Body=io.BytesIO(decoded_bytes),
            )
            print(f'{png_key} subido correctamente a MinIO.')
            
        except Exception as e:
            print(f"Error: {str(e)}")
            return

    #Subir a minIO el recurso thumbnail
    if(input_data['clipped'] is not None):
        try:
            
            s3_client = get_minio_client()
            bucket_name = 'missions'  
            png_key = str(uuid_key) + '/' + 'vegetation_detection_clipped' + '.png'
            decoded_bytes = base64.b64decode(input_data['clipped'].split(",")[1])

            # Subir el archivo a MinIO
            s3_client.put_object(
                Bucket=bucket_name,
                Key=png_key,
                Body=io.BytesIO(decoded_bytes),
            )
            print(f'{png_key} subido correctamente a MinIO.')
            
        except Exception as e:
            print(f"Error: {str(e)}")
            return 


        timeforseconds = second_in_time(input_data['time_seconds'])
        print(second_in_time(input_data['time_seconds']))

        if input_data['video_id'] is not None:
            print(f"id_video: {input_data['video_id']}")

            #Actualizamos la base de datos
            try:
                session = get_db_session()
                engine = session.get_bind()

                query = text("""
                   INSERT INTO missions.mss_inspection_detection_frame_incidence
                    (video_id, resource_id, element_type_id, incidence_type_id, frame_timestamp, region_px, notes, geometry)
                    VALUES(:videoID, :resourceId, :elementType, :incidenceId, :frame_timestamp, :region, :notes, ST_SetSRID(ST_MakePoint(0, 0), 4326));
                """)
                result = session.execute(query, {'videoID': input_data['video_id'] , 'resourceId': uuid_key, 'elementType': input_data['element_type'],
                                                 'incidenceId': input_data['incidence_type'],
                                                 'frame_timestamp': timeforseconds,
                                                 'region': input_data['region_px'],
                                                 'notes': input_data['notes'] })
                session.commit()
                print("Inserción exitosa")
                print(f"Número de filas afectadas: {result.rowcount}")

            except Exception as e:
                session.rollback()
                print(f"Error durante la busqueda : {str(e)}")


            finally:
                session.close()
                
        else:
            print("review_status no está presente o es None")



def second_in_time(secondstime):
    hours, remainder = divmod(secondstime, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    frame_timestamp = datetime.time(int(hours), int(minutes), int(seconds))
    return frame_timestamp



def change_state_job(**context):
    message = context['dag_run'].conf
    job_id = message['message']['id']
    print(f"jobid {job_id}" )

    try:
   
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        session = get_db_session()
        engine = session.get_bind()


        # Update job status to 'FINISHED'
        metadata = MetaData(bind=engine)
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED', input_data=json.dumps({}))
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado del estado del job: {str(e)}")

 

def generate_notify_job(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    
    # Convertir la cadena de input_data en un diccionario
    input_data = json.loads(input_data_str)

    #Buscamos la carpeta correspondiente
    try:
        session = get_db_session()
        engine = session.get_bind()

        query = text("""
            SELECT mi.mission_id
            FROM missions.mss_mission_inspection mi
            JOIN missions.mss_inspection_video vp ON vp.mission_inspection_id = mi.id               
            WHERE vp.id = :video_id;
        """)
        result = session.execute(query, {'video_id': input_data['video_id']})
        row = result.fetchone()
        if row is not None:
            mission_id = row[0]
            print(f"Resource ID: {mission_id}")
        else:
            mission_id = None
            print("No se encontró el mission_id por lo que no se puede completar la notificación")
            return None

    except Exception as e:
        session.rollback()
        print(f"Error durante la busqueda del mission_inspection: {str(e)}")
    finally:
        session.close()

    if mission_id is not None:
        #Añadimos notificacion
        
        try:
            session = get_db_session()
            engine = session.get_bind()

            data_json = json.dumps({
                "to":"all_users",
                "actions":[{
                    "type":"reloadMission",
                    "data":{
                        "missionId":mission_id
                    }
                }]
            })
            time = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)

            query = text("""
                INSERT INTO public.notifications
                (destination, "data", "date", status)
                VALUES (:destination, :data, :date, NULL);
            """)
            session.execute(query, {
                'destination': 'inspection',
                'data': data_json,
                'date': time
            })
            session.commit()

        except Exception as e:
            session.rollback()
            print(f"Error durante la inserción de la notificación: {str(e)}")
        finally:
            session.close()

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

dag = DAG(
    'mission_inspection_video_review_postprocess_and_notification',
    default_args=default_args,
    description='Algoritmo video_detection',
    schedule_interval=None,
    catchup=False
)

process_element_task = PythonOperator(
    task_id='process_message',
    python_callable=process_element,
    provide_context=True,
    dag=dag,
)

#Cambia estado de job
change_state_task = PythonOperator(
    task_id='change_state_job',
    python_callable=change_state_job,
    provide_context=True,
    dag=dag,
)

#Generar notificación de vuelta
generate_notify = PythonOperator(
    task_id='generate_notify_job',
    python_callable=generate_notify_job,
    provide_context=True,
    dag=dag,
)


process_element_task >> change_state_task >> generate_notify