from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

# Configuración del DAG de monitoreo
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 7),
    'retries': 1,
}

dag = DAG(
    'monitor_dags',
    default_args=default_args,
    description='DAG que monitorea otros DAGs y muestra un mensaje cuando terminan',
    schedule_interval=None,  # Se ejecuta manualmente
)

# Tarea inicial
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Crear un TaskSensor y un PythonOperator para cada DAG a monitorear
for dag_name in dag_names_to_monitor:
    # Sensor que espera la tarea final de cada DAG
    wait_for_task = ExternalTaskSensor(
        task_id=f'wait_for_{dag_name}_final_task',
        external_dag_id=dag_name,
        external_task_id='final_task',  # La tarea que marca el final del DAG monitoreado
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
    start >> wait_for_task >> print_msg_task
