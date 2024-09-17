import base64
from datetime import datetime, timedelta, timezone
import io
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import json
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, MetaData, text
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker
import boto3
from botocore.client import Config

def process_element(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    
    # Convertir la cadena de input_data en un diccionario
    input_data = json.loads(input_data_str)

    # Verifica si el JSON contiene la clave "resources"
    if 'resources' in input_data:
        resources = input_data['resources']
        if input_data.get('conflict_id') is not None:
            # Iterar sobre la lista de recursos y acceder a los datos
            # Acceder a conflict_id y review_status si existen
            conflict_id = input_data.get('conflict_id')
            print(f"conflict_id: {conflict_id}")


            #Buscamos la carpeta del conflicto correspondiente
            try:
                db_conn = BaseHook.get_connection('biobd')
                connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                engine = create_engine(connection_string)
                Session = sessionmaker(bind=engine)
                session = Session()

                query = text("""
                    SELECT vc.resource_id
                    FROM missions.mss_inspection_vegetation_conflict c
                    JOIN missions.mss_inspection_vegetation_child vc
                    ON c.vegetation_child_id = vc.id
                    WHERE c.id = :conflict_id;
                """)
                result = session.execute(query, {'conflict_id': conflict_id})
                row = result.fetchone()
                if row is not None:
                    resource_id = row[0]
                    print(f"Resource ID: {resource_id}")
                else:
                    resource_id = None
                    print("No se encontró el conflict_id en la tabla mss_inspection_vegetation_conflict")
                    return None

            except Exception as e:
                session.rollback()
                print(f"Error durante la busqueda del mission_inspection: {str(e)}")


            index = 1
            uuid = uuid.uuid4()
            for resource in resources:
                data = resource.get('data')
                
                if data:
                    #Subimos a esa carpeta los nuevos elementos
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

                        bucket_name = 'missions'  
                        pdf_key = str(uuid) + '/' + 'vegetation_review_incidence' + str(index) + '.png'
                        index = index + 1
                        print(data[-10:])
                        decoded_bytes = base64.b64decode(data.split(",")[1])
                        print(decoded_bytes[-10:])

                        # Subir el archivo a MinIO
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=pdf_key,
                            Body=io.BytesIO(decoded_bytes),
                        )
                        print(f'{pdf_key} subido correctamente a MinIO.')
                    except Exception as e:
                        print(f"Error: {str(e)}")
                else:
                    print("data no está presente o es None")
                

            review_status = input_data.get('review_status')
            if review_status is not None:
                print(f"review_status: {review_status}")

                #Actualizamos el review_status
                try:
                    db_conn = BaseHook.get_connection('biobd')
                    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                    engine = create_engine(connection_string)
                    Session = sessionmaker(bind=engine)
                    session = Session()

                    query = text("""
                        UPDATE missions.mss_inspection_vegetation_conflict
                        SET review_status_id = :new_review_status,
                        resource_id = :new_resource_id
                        WHERE id = :conflict_id
                        RETURNING *;
                    """)
                    result = session.execute(query, {'new_review_status': review_status, 'conflict_id': conflict_id, 'new_resource_id': uuid})
                    row = result.fetchone()
                    if row is not None:
                        print(f"Fila actualizada: {row}")
                    else:
                        print(f"No se encontró el conflict_id {conflict_id} en la tabla mss_inspection_vegetation_conflict")
                    session.commit()

                except Exception as e:
                    session.rollback()
                    print(f"Error durante la busqueda del mission_inspection: {str(e)}")

                finally:
                    session.close()
                
            else:
                print("review_status no está presente o es None")
        else:
            print("El json no contiene un id de conflicto, por lo que no se puede ejecutar") 
    else:
        print("El mensaje no es correcto, no se puede realizar la subida de los datos entregados")
  
  
  
  



     


def fix_base64_padding(data):
    # Asegura que la longitud de la cadena sea un múltiplo de 4
    return data + '=' * (-len(data) % 4)

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

    # Verifica si el JSON contiene la clave "resources"
    if 'resources' in input_data:
        if input_data.get('conflict_id') is not None:
            print(input_data.get('conflict_id'))
            conflict_id = input_data.get('conflict_id')

            #Buscamos la carpeta del conflicto correspondiente
            try:
                db_conn = BaseHook.get_connection('biobd')
                connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                engine = create_engine(connection_string)
                Session = sessionmaker(bind=engine)
                session = Session()

                query = text("""
                    SELECT mi.mission_id
                    FROM missions.mss_mission_inspection mi
                    JOIN missions.mss_inspection_vegetation_parent vp ON vp.mission_inspection_id = mi.id
                    JOIN missions.mss_inspection_vegetation_child vc ON vc.vegetation_parent_id = vp.id
                    JOIN missions.mss_inspection_vegetation_conflict vconf ON vconf.vegetation_child_id = vc.id
                    WHERE vconf.id = :conflict_id;
                """)
                result = session.execute(query, {'conflict_id': conflict_id})
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
                    db_conn = BaseHook.get_connection('biobd')
                    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                    engine = create_engine(connection_string)
                    Session = sessionmaker(bind=engine)
                    session = Session()

                    data_json = json.dumps({
                        "to":"all_users",
                        "actions":[{
                            "type":"reloadMission",
                            "data":{
                                "missionId":mission_id
                            }
                        }]
                    })
                    time = datetime.now().replace(tzinfo=timezone.utc)

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
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'vegetation_review_incidence',
    default_args=default_args,
    description='Algoritmo vuelta del potree',
    schedule_interval=None,
    catchup=False
)

# Manda correo
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