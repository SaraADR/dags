from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
import ast
import json
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import io
import json
import tempfile
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
import ast
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
from airflow.operators.email import EmailOperator


def process_element(**context, ):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    menssage_str = message['message']

    input_data = json.loads(input_data_str)

    print(f"Todo {input_data['input']}")   
    location = input_data['input']['location']
    print(f"Location: {location}")
    perimeter = input_data['input']['perimeter']
    print(f"Perimeter: {perimeter}")
    emails = input_data['emails']
    print(f"Emails: {emails}")    

    # Imprime las propiedades
    print(f"--------------------")    
    print(f"Location: {location}")
    print(f"Perimeter: {perimeter}")
    print(f"Emails: {emails}")    

    text = f"Esto es un texto dentro del pdf. La localización es la siguiente: {location} y el perímetro es: {perimeter}"
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


    try:
        # Configuración del cliente de MinIO
        connection = BaseHook.get_connection('minio_conn')
        extra = json.loads(connection.extra)
        s3_client = boto3.client(
            's3',
            endpoint_url=extra['endpoint_url'],
            aws_access_key_id=extra['aws_access_key_id'],
            aws_secret_access_key=extra['aws_secret_access_key'],
            config=Config(signature_version='s3v4')
        )

        # Obtener el PDF desde MinIO
        pdf_obj = s3_client.get_object(Bucket=bucket_name, Key=pdf_key)
        pdf_content = pdf_obj['Body'].read()

        # Enviar correos electrónicos
        for email in emails:
            email = email.replace("'", "")
            email_operator = EmailOperator(
                task_id=f'send_email_{email}',
                to=email,
                subject='Tu archivo PDF',
                html_content='<p>Adjunto encontrarás el PDF generado.</p>',
                files=[pdf_content],  
                conn_id='test_mailing',
                dag=context['dag']
            )
            email_operator.execute(context)
    except Exception as e:
        print(f"Error: {str(e)}")    
     
    


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
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_automaps',
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


process_element_task