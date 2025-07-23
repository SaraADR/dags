import json
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker
import datetime
from airflow import DAG
from sqlalchemy import create_engine, Table, MetaData, text
from dag_utils import get_db_session


def check_jobs_status(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']

    # Convertir la cadena de input_data en un diccionario
    input_data = json.loads(input_data_str)
    job_ids = input_data['job_ids']

    try:
        
        session = get_db_session()
        engine = session.get_bind()
        
        # Consulta para obtener los estados de los jobs
        jobs_query = session.execute(f"SELECT id, status FROM PUBLIC.jobs WHERE id IN ({','.join(map(str, job_ids))})")
        job_statuses = {job.id: job.status for job in jobs_query}
        
        # Verificar si todos los jobs están en estado 'finished'
        all_finished = all(status == 'FINISHED' for status in job_statuses.values())
        session.close()
        
        return all_finished

    except Exception as e:
            print(f"Error: {str(e)}")
            return



def update_video_status(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']

    input_data = json.loads(input_data_str)
    video_id = input_data['video_id']
    
    session = get_db_session()
    engine = session.get_bind()


    # Actualizar el campo video a True en la tabla correspondiente
    session.execute(f"UPDATE missions.mss_inspection_video SET reviewed = True WHERE id = {video_id}")
    session.commit()
    session.close()




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
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado del estado del job: {str(e)}")

 

def generate_notify_job(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']

    input_data = json.loads(input_data_str)
    video_id = input_data['video_id']
    

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
        result = session.execute(query, {'video_id': video_id})
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

def always_save_logs(**context):
    return True


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
    'mission_inspection_video_revision_monitor_and_job_update',
    default_args=default_args,
    description='Proceso listen-detections-finished',
    schedule_interval=None,
    catchup=False
)

wait_for_jobs_sensor = PythonSensor(
    task_id='wait_for_jobs_to_finish',
    python_callable=check_jobs_status,
    timeout=30 * 60, 
    poke_interval=20,  # Revisar cada 20 segundos
    mode='poke',  
    dag=dag,
)

#Actualizar el estado del video
update_video_task = PythonOperator(
    task_id='update_video_status',
    python_callable=update_video_status,
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



# Definir dependencias
wait_for_jobs_sensor >> update_video_task >> change_state_task >> generate_notify