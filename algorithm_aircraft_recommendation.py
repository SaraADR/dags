from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

def print_input_data(**context):
    input_data = context['dag_run'].conf.get('inputData', {})
    print("\n inputData recibido:")
    print(json.dumps(input_data, indent=2))

    mission_data = input_data.get('missionData', [])
    print(f"➡️ Total misiones recibidas: {len(mission_data)}")

    for i, mission in enumerate(mission_data, start=1):
        print(f"\n Misión #{i}:")
        print(json.dumps(mission, indent=2))


# Configuración estandarizada de DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': True,
}

dag = DAG(
    'algorithm_aircraft_recommendation',
    default_args=default_args,
    description='DAG para mostrar y validar inputData recibido desde el frontend para el algoritmo recomendador',
    schedule_interval=None,
    catchup=False,
    max_active_runs=2,
    concurrency=2,
)

log_input_data = PythonOperator(
    task_id='log_front_input',
    python_callable=print_input_data,
    provide_context=True,
    dag=dag,
)

log_input_data