from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

def printOK():
    print("Esto funciona")

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

dag = DAG(
    'algorithm_metashape',
    default_args=default_args,
    description='TODO',
    schedule_interval=None,  # Se puede ajustar según necesidades
    catchup=False
)

# Tarea 3: Subir el XML a GeoNetwork
upload_xml_task = PythonOperator(
    task_id='upload_to_geonetwork',
    python_callable=printOK, #Importante
    provide_context=True,
    dag=dag
)

# Definir el flujo de las tareas
upload_xml_task
# get_user_task >>