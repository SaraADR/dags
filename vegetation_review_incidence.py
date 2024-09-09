from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import io
import json
import tempfile
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker

def process_element(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    menssage_str = message['message']

    input_data = json.loads(input_data_str)

    print(f"Todo {input_data['resources']}")   
    conflict_id = input_data['resources']['conflict_id']
    print(f"conflict_id: {conflict_id}")
    review_status = input_data['resources']['review_status']
    print(f"review_status: {review_status}")

    data = input_data['resources']['data']
    print(f"data: {data}")

    #Buscar el child correspondiente y traernos su carpeta de recursos
    # try:
    #     connection = BaseHook.get_connection('minio_conn')
    #     extra = json.loads(connection.extra)
    #     s3_client = boto3.client(
    #         's3',
    #         endpoint_url=extra['endpoint_url'],
    #         aws_access_key_id=extra['aws_access_key_id'],
    #         aws_secret_access_key=extra['aws_secret_access_key'],
    #         config=Config(signature_version='s3v4')
    #     )

    #     bucket_name = 'avincis-test'  
    #     time = datetime.now().replace(tzinfo=timezone.utc)
    #     pdf_key = str(uuid.uuid4()) + '/' + 'automaps_pdf_generado' + time.strftime('%Y-%m-%d %H:%M:%S %Z') + '.pdf'

    #     # Subir el archivo a MinIO
    #     s3_client.put_object(
    #         Bucket=bucket_name,
    #         Key=pdf_key,
    #         Body=pdf_buffer,
    #         ContentType='application/pdf'
    #     )
    #     print(f'{pdf_key} subido correctamente a MinIO.')
    # except Exception as e:
    #     print(f"Error: {str(e)}")

     


    

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

process_element_task >> change_state_task