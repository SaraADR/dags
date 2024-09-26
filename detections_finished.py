
import json
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker
import datetime
from airflow import DAG
from sqlalchemy import create_engine, Table, MetaData


def check_jobs_status(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']

    # Convertir la cadena de input_data en un diccionario
    input_data = json.loads(input_data_str)
    job_ids = input_data['job_ids']

    try:
        
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        
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
    
    db_conn = BaseHook.get_connection('biobd')
    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()


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
    'detections_finished',
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

# Definir dependencias
wait_for_jobs_sensor >> update_video_task >> change_state_task