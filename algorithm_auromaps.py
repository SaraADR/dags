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


def process_element(**context, ):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    menssage_str = message['message']

    input_data = json.loads(input_data_str)

    location = input_data['input']['location']
    perimeter = input_data['input']['perimeter']
    emails = input_data['input']['emails']

    # Imprime las propiedades
    print(f"Location: {location}")
    print(f"Perimeter: {perimeter}")
    print(f"Emails: {emails}")    

    text = context.get('text', 'Esto es un texto dentro del pdf' + 'la localización es la siguiente ' + location + ' y el perimetro este ' + perimeter )
    pdf_buffer = generate_pdf_in_memory(text)

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
    pdf_key = str(uuid.uuid4()) + '/' + 'pdf_generado' + datetime.now().replace(tzinfo=timezone.utc)

    # Subir el archivo a MinIO
    s3_client.put_object(
        Bucket=bucket_name,
        Key=pdf_key,
        Body=pdf_buffer,
        content_type='application/pdf'
    )
    print(f'{pdf_key} subido correctamente a MinIO.')





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
    task_id='print_message',
    python_callable=process_element,
    provide_context=True,
    dag=dag,
)


process_element_task