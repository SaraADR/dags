from airflow import DAG
from airflow.providers.apache.airflow.operators.dummy import DummyOperator
from airflow.providers.apache.airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime

# DAGs a monitorear
dag_names_to_monitor  = [
    'algorithm_dNBR_process_Type1',
    'kafka_consumer_classify_files_and_trigger_dags',
    'kafka_consumer_trigger_jobs',
]
# Función para imprimir el mensaje cuando un DAG termina
def print_message(dag_name):
    print(f"DAG {dag_name} TERMINADO")

# Definir el DAG de monitoreo
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 7),
    'retries': 1,
}

dag = DAG(
    'monitor_dags',
    default_args=default_args,
    description='Un DAG que monitorea otros DAGs y muestra un mensaje cuando terminan',
    schedule_interval=None,  # Este DAG no se ejecuta automáticamente, se ejecuta a mano
)

# Tarea dummy que marca el inicio del DAG
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Crear un ExternalTaskSensor y un PythonOperator para cada DAG a monitorear
for dag_name in dag_names_to_monitor:
    # Sensor que espera a que el DAG termine
    wait_for_dag = ExternalTaskSensor(
        task_id=f'wait_for_{dag_name}',
        external_dag_id=dag_name,
        mode='poke', 
        poke_interval=60,  
        timeout=600,  
        retries=3,
        dag=dag,
    )

    # Tarea para imprimir el mensaje cuando el DAG haya terminado
    print_msg_task = PythonOperator(
        task_id=f'print_message_{dag_name}',
        python_callable=print_message,
        op_args=[dag_name],
        dag=dag,
    )

    # Definir la secuencia de tareas
    start >> wait_for_dag >> print_msg_task