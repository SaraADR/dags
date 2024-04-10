from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    "owner": "Sadr",
    "depend_on_past": False,
    "start_date": datetime(2024, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "api-key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzYXJhLmFycmliYXNAY3VhdHJvZGlnaXRhbC5jb20iLCJqdGkiOiIyOWMyZjJkMi1hNWM2LTQ4NmYtYWNhZC0xZTY1NjhiNWEwYzUiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTcxMjc0MzEyOSwidXNlcklkIjoiMjljMmYyZDItYTVjNi00ODZmLWFjYWQtMWU2NTY4YjVhMGM1Iiwicm9sZSI6IiJ9.Fev0ADUIPt-NBmMLDIEqrybWG9MUsKU12U_G2CyAo_4"
}

def generate_json():
    # Datos de ejemplo
    data = {
        "coordenadas": {
            "x": 10.01,
            "y": 14.34 
        },
        "inicio_periodo": "14:05:02 10-05-2024",
        "fin_periodo": "16:00:00 12-05-2024",
        "extension_tiff": "?",
        "fichero_tiff": False,
        "horas_periodo": 1
    }
    json_data = json.dumps(data)
    return json_data

def save_to_mongodb(json_data, **kwargs):
    # Establecer conexión con MongoDB
    hook = MongoHook(mongo_conn_id='mongoid')
    client = hook.get_conn()
    db = client['airflow']
    collection = db['datalake']
    # Insertar el JSON en la colección
    collection.insert_one(json.loads(json_data))
    print(f"Connected to MongoDB - {client.server_info()}")

def procedimiento_inicial_def(json_data, **kwargs):
    data = json.loads(json_data)
    if "inicio_periodo" in data:
        print("El campo 'inicio periodo' existe.")
    else:
        print("El campo 'inicio periodo' no existe.")
        data.setdefault("inicio_periodo", datetime.now().isoformat())

    if "fin_periodo" in data:
        print("El campo 'fin periodo' existe.")
    else:
        print("El campo 'fin periodo' no existe.")
        data["fin_periodo"] = (datetime.now() + timedelta(hours=6)).isoformat()
    print(data)
    return data

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

procedimiento_inicial = PythonOperator(
    task_id='procedimiento_inicial',
    python_callable=procedimiento_inicial_def,
    op_kwargs={'json_data': generate_json()},  # Pasa el resultado de generate_json como argumento
    dag=dag,
)

# Establece la secuencia de tareas
generate_json_task >> save_to_mongodb_task >> procedimiento_inicial
