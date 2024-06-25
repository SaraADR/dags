from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka_consumer import get_kafka_consumer, consume_kafka_message
import json
import base64
from PIL import Image
from io import BytesIO

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 23),
    'retries': 1,
}

def process_kafka_message():
    connection_id = 'kafka_connection'  # Nombre de la conexión en Airflow
    topic = 'test1'  # Reemplaza con tu tema de Kafka
    
    consumer = get_kafka_consumer(connection_id)
    print("GET CONSUMER")

    message = consume_kafka_message(consumer, topic)
    print("CONSUME CONSUMER")

    if message:
        data = json.loads(message)
        image_data = base64.b64decode(data['image_base64'])
        image = Image.open(BytesIO(image_data))
        
        # Extraer metadatos de la imagen
        metadata = {
            "format": image.format,
            "mode": image.mode,
            "size": image.size,
        }
        
        # Imprimir los metadatos en la consola
        print("Metadatos de la imagen:", metadata)
        
        return metadata

with DAG('process_image_dag', default_args=default_args, schedule_interval='@once') as dag:
    task = PythonOperator(
        task_id='process_kafka_message',
        python_callable=process_kafka_message,
    )
