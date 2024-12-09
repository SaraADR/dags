from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import uuid
import boto3
from botocore.client import Config
from airflow.hooks.base_hook import BaseHook
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData, text
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.orm import sessionmaker
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
from airflow.providers.ssh.hooks.ssh import SSHHook
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np



def process_escape_routes_data(**context):
    # Obtener los datos del contexto del DAG
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)

    # Extraer los argumentos necesarios
    params = {
        "destino": input_data.get('destino', None),
        "direccion_avance": input_data.get('direccion_avance', None),
        "distancia": input_data.get('distancia', None),
        "dir_obstaculos": input_data.get('dir_obstaculos', None),
        "dir_carr_csv": input_data.get('dir_carr_csv', None),
        "zonas_abiertas": input_data.get('zonas_abiertas', None),
        "v_viento": input_data.get('v_viento', None),
        "f_buffer": input_data.get('f_buffer', 100),  # Valor predeterminado 100
        "c_prop": input_data.get('c_prop', "Extremas"),  # Valor predeterminado "Extremas"
        "lim_pendiente": input_data.get('lim_pendiente', None),
        "dist_estudio": input_data.get('dist_estudio', 5000),  # Valor predeterminado 5000
    }

    # Crear el JSON dinámicamente
    json_data = create_json(params)

    # Mostrar el JSON por pantalla
    print("JSON generado:")
    print(json.dumps(json_data, indent=4))

def create_json(params):
    # Generar un JSON basado en los parámetros proporcionados
    input_data = {
        "destino": params.get("destino", None),
        "direccion_avance": params.get("direccion_avance", None),
        "distancia": params.get("distancia", None),
        "dir_obstaculos": params.get("dir_obstaculos", None),
        "dir_carr_csv": params.get("dir_carr_csv", None),
        "zonas_abiertas": params.get("zonas_abiertas", None),
        "v_viento": params.get("v_viento", None),
        "f_buffer": params.get("f_buffer", 100),
        "c_prop": params.get("c_prop", "Extremas"),
        "lim_pendiente": params.get("lim_pendiente", None),
        "dist_estudio": params.get("dist_estudio", 5000),
    }
    return input_data

# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_escape_routes_post_process',
    default_args=default_args,
    description='DAG para generar JSON y mostrarlo por pantalla',
    schedule_interval=None,
    catchup=False,
    concurrency=1
)

# Tarea para generar y mostrar el JSON
process_escape_routes_task = PythonOperator(
    task_id='process_escape_routes',
    provide_context=True,
    python_callable=process_escape_routes_data,
    dag=dag,
)

# Cambiar el estado del job (si fuera necesario en un futuro)
change_state_task = PythonOperator(
    task_id='change_state_job',
    python_callable=lambda: print("Cambio de estado no implementado."),
    provide_context=True,
    dag=dag,
)

# Definir el flujo de tareas
process_escape_routes_task >> change_state_task
