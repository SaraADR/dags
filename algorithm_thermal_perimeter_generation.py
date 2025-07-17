import json
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from dag_utils import throw_job_error, update_job_status
from airflow import DAG



def execute_thermal_perimeter_process(**context):
    """Ejecuta el algoritmo de perímetros térmicos utilizando los datos recibidos desde la interfaz."""

    conf = context.get("dag_run").conf
    if not conf:
        print("Error: No se recibió configuración desde el DAG.")
        return
    
    print("Datos recibidos del DAG:")
    print(json.dumps(conf, indent=4))

    message = conf.get("message", {})
    trace_id = conf.get("trace_id", "no-trace")
    print(f"Processing with trace_id: {trace_id}")
    
    job_id = message.get('id')
    input_data_str = message.get('input_data', '{}')
    
    try:
        input_data = json.loads(input_data_str)
    except json.JSONDecodeError:
        print("Error al decodificar 'input_data'")
        throw_job_error(job_id, "Invalid JSON in input_data")
        return

    # Extraer datos de la interfaz
    mission_id = input_data.get('mission_id')
    selected_bursts = input_data.get('selected_bursts', [])  # IDs de ráfagas seleccionadas
    
    if not mission_id:
        throw_job_error(job_id, "No se especificó ID de misión")
        return
    
    if not selected_bursts:
        throw_job_error(job_id, "No se seleccionaron ráfagas")
        return
    
def change_job_status(**context):
    """Cambia el estado del job a FINISHED."""
    message = context['dag_run'].conf['message']
    job_id = message['id']
    update_job_status(job_id, 'FINISHED')

# Configuración del DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


dag = DAG(
    'algorithm_thermal_perimeter_generation',
    default_args=default_args,
    description='DAG para generar perímetros de incendios a partir de imágenes termográficas seleccionadas en la interfaz',
    schedule_interval=None,  # Triggered by Kafka
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# Tarea principal del algoritmo
execute_algorithm_task = PythonOperator(
    task_id='execute_thermal_perimeter_algorithm',
    python_callable=execute_thermal_perimeter_process,
    provide_context=True,
    dag=dag,
)

# Cambiar estado a finalizado
change_status_task = PythonOperator(
    task_id='change_job_status_to_finished',
    python_callable=change_job_status,
    provide_context=True,
    dag=dag,
)

# Flujo del DAG
execute_algorithm_task >> change_status_task


    

