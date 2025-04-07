from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

# Función que imprime un mensaje cuando un DAG termina
def print_message(**kwargs):
    dag_name = kwargs['dag_run'].conf.get('dag_name', 'Desconocido')
    print(f"DAG {dag_name} TERMINADO")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 7),
    'retries': 1,
}

dag = DAG(
    'monitor_dags',
    default_args=default_args,
    description='Monitorea otros DAGs y muestra un mensaje cuando terminan',
    schedule_interval=None,
)

print_msg_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    provide_context=True,
    dag=dag,
)


print_msg_task
