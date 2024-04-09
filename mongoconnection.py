from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 9),
    'retries': 1,
}

def generate_json():
    # Datos de ejemplo
    data = {
        "nombre": "Ejemplo",
        "edad": 30,
        "ciudad": "Ejemplolandia"
    }
    json_data = json.dumps(data)
    return json_data

def save_to_mongodb(json_data, **kwargs):
    # Establecer conexión con MongoDB
    hook = MongoHook(mongo_conn_id='mongoid')
    client = hook.get_conn()
    print(f" MongoDB - {client}")
    db = client['airflow']
    print(f" MongoDB - {db}")
    collection = db['datalake']
    print(f" MongoDB - {collection}")
    # Insertar el JSON en la colección
    collection.insert_one(json.loads(json_data))

dag = DAG(
    'json_to_mongodb',
    default_args=default_args,
    description='Genera un JSON y lo guarda en MongoDB',
    schedule_interval='@once',
)

# Define los operadores en el DAG
generate_json_task = PythonOperator(
    task_id='generate_json',
    python_callable=generate_json,
    dag=dag,
)

save_to_mongodb_task = PythonOperator(
    task_id='save_to_mongodb',
    python_callable=save_to_mongodb,
    op_kwargs={'json_data': generate_json()},  # Pasa el resultado de generate_json como argumento
    dag=dag,
)

# Establece la secuencia de tareas
generate_json_task >> save_to_mongodb_task
