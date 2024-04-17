from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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

#Json de ejemplo
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
        #"horas_periodo": 1
    }
    json_data = json.dumps(data)
    return json_data

#Guardamos lo que sea que nos llegue como dato inicial en mongobd
def save_to_mongodb(json_data, **kwargs):
    # Establecer conexión con MongoDB
    hook = MongoHook(mongo_conn_id='mongoid')
    client = hook.get_conn()
    db = client['airflow']
    collection = db['datalake']
    # Insertar el JSON en la colección
    collection.insert_one(json.loads(json_data))
    print(f"Connected to MongoDB - {client.server_info()}")

#Caracteristicas iniciales, si campo inicio o fin no estan se rellenan
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
        if "horas_periodo" in data:
            data["fin_periodo"] = (datetime.now() + timedelta(data["horas_periodo"])).isoformat()
        else:    
            data["fin_periodo"] = (datetime.now() + timedelta(hours=6)).isoformat()
    print(data)
    return data

#Se calcula el uso de las coordenadas
def calculo_huso(json_data, **kwargs):
    coordLong = json_data['coordenadas']["x"]
    return int((coordLong / 6) + 31)

#Se calcula la dirección del viento
def calculo_wind_direction(json_data, **kwargs):
    if dir == "N":
        return 0
    elif dir == "NE":
        return 45
    elif dir == "E":
        return 90
    elif dir == "SE":
        return 135
    elif dir == "S":
        return 180
    elif dir == "SO":
        return 225
    elif dir == "O":
        return 270
    elif dir == "NO":
        return 315
    elif dir == "C":
        return 0  # Consideramos "calma" como 0 grados



dag = DAG(
    'vientos',
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
    op_kwargs={'json_data': generate_json()}, 
    dag=dag,
)

procedimiento_inicial = PythonOperator(
    task_id='procedimiento_inicial',
    python_callable=procedimiento_inicial_def,
    op_kwargs={'json_data': generate_json()},  
    dag=dag,
)

huso = PythonOperator(
    task_id='huso',
    python_callable=calculo_huso,
    op_kwargs={'json_data': procedimiento_inicial.output},  
    dag=dag,
)

wind_direction = PythonOperator(
    task_id='direction',
    python_callable=calculo_wind_direction,
    op_kwargs={'json_data': procedimiento_inicial.output},  
    dag=dag,
)

# downloadAEMET = PythonOperator(
#     task_id='api_key',
#     python_callable=aemetdownload,
#     op_kwargs={'json_data': procedimiento_inicial.output , 'huso' : huso.output},  
#     dag=dag,
# )

downloadAEMET = SparkSubmitOperator(
    task_id='spark',
    conn_id='spark2',
    application='/opt/airflow/dags/repo/archivos/spark1.py',
    executor_memory='2g',
    executor_cores='1',
    total_executor_cores='1',
    num_executors='1',
    driver_memory='2g',
    verbose=False,
    application_args=[procedimiento_inicial.output , huso.output],
)

# Establece la secuencia de tareas
generate_json_task >> save_to_mongodb_task >> [procedimiento_inicial, huso] >> downloadAEMET >> wind_direction 
