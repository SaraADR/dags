from datetime import datetime, timedelta, timezone
from airflow.operators.python import PythonOperator
import json
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import io
import tempfile
import uuid
from airflow import DAG
from datetime import datetime, timedelta
import os
import boto3
from botocore.client import Config
from airflow.operators.email import EmailOperator
from sqlalchemy import create_engine, Table, MetaData
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker

def process_element(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    menssage_str = message['message']

    input_data = json.loads(input_data_str)

    print(f"Todo {input_data['input']}")   
    location = input_data['input']['location']
    print(f"Location: {location}")
    if 'perimeter' in input_data['input'] and input_data['input']['perimeter'] is not None:
        perimeter = input_data['input']['perimeter']
        print(f"Perimeter: {perimeter}")
    emails = input_data['emails']
    print(f"Emails: {emails}")    


    text = f"La localización es la siguiente: {location}"
    pdf_buffer = generate_pdf_in_memory(text)


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

        bucket_name = 'avincis-test'  
        time = datetime.now().replace(tzinfo=timezone.utc)
        pdf_key = str(uuid.uuid4()) + '/' + 'automaps_pdf_generado' + time.strftime('%Y-%m-%d %H:%M:%S %Z') + '.pdf'

        # Subir el archivo a MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=pdf_key,
            Body=pdf_buffer,
            ContentType='application/pdf'
        )
        print(f'{pdf_key} subido correctamente a MinIO.')
    except Exception as e:
        print(f"Error: {str(e)}")

     
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
        tmp_file.write(pdf_buffer.getvalue())  
        tmp_file_path = tmp_file.name

        # Enviar correos electrónicos
    try:
        # Enviar correos electrónicos
        for email in emails:
            email = email.replace("'", "")
            email_operator = EmailOperator(
                task_id=f'send_email_{email.replace("@", "-")}',
                to=email,
                subject='Automaps ha generado un archivo ',
                html_content='<p>Adjunto encontrarás el PDF generado.</p>',
                files=[tmp_file_path],
                conn_id='test_mailing',
                dag=context['dag']
            )
            email_operator.execute(context)
    except Exception as e:
        print(f"Error: {str(e)}")    
    finally:
        # Eliminar el archivo temporal
        os.remove(tmp_file_path)


    

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
        session.close()
        print(f"Job ID {job_id} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado del estado del job: {str(e)}")

 



#Metodo auxiliar que genera un pdf en memoria (luego conectara con el docker)
def generate_pdf_in_memory(text):
    """Genera un PDF en memoria con el texto proporcionado."""
    pdf_buffer = io.BytesIO()
    c = canvas.Canvas(pdf_buffer, pagesize=letter)
    width, height = letter
    
    c.drawString(100, height - 100, text)
    c.save()
    
    pdf_buffer.seek(0)
    
    return pdf_buffer



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
    'algorithm_automaps_store_and_notify',
    default_args=default_args,
    description='Algoritmo automaps',
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