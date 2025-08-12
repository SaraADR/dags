import os
import uuid
from datetime import datetime, timezone
from airflow.operators.python import get_current_context
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from dag_utils import get_minio_client, download_from_minio, delete_file_sftp
from utils.kafka_headers import extract_trace_id

KAFKA_RAW_MESSAGE_PREFIX = "Mensaje crudo:"

def there_was_kafka_message(task_id='consume_from_topic', **context):
    """
    Verifica si hubo mensajes Kafka en los logs.
    
    Args:
        task_id: El ID de la tarea a verificar (por defecto 'consume_from_topic')
        **context: Contexto de Airflow con dag_id y run_id
    
    Returns:
        bool: True si se encontraron mensajes Kafka, False en caso contrario
    """
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    log_base = "/opt/airflow/logs"
    log_path = f"{log_base}/dag_id={dag_id}/run_id={run_id}/task_id={task_id}"
    
    # Search for the latest log file
    try:
        latest_log = max(
            (os.path.join(root, f) for root, _, files in os.walk(log_path) for f in files),
            key=os.path.getctime
        )
        with open(latest_log, 'r') as f:
            content = f.read()
            return f"{KAFKA_RAW_MESSAGE_PREFIX} <cimpl.Message object at" in content
    except (ValueError, FileNotFoundError):
        return False
    
def store_trace_id_in_xcom(trace_id):
    """Store trace_id in XCom"""
    try:
        context = get_current_context()
        if context and 'task_instance' in context:
            context['task_instance'].xcom_push(key='trace_id', value=trace_id)
            print(f"trace_id guardado en XCom: {trace_id}")
    except Exception as e:
        print(f"Error al guardar trace_id en XCom: {e}")