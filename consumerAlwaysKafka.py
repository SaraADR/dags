import json
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

def handle_message(message, **kwargs):
    try:
        # Intentamos convertir el mensaje a JSON
        msg_dict = json.loads(message)
    except json.JSONDecodeError:
        # Si no es un JSON válido, devolvemos None
        return None
    
    # Verificamos si el mensaje contiene el campo 'destination' con el valor 'email'
    if msg_dict.get('destination') == 'email':
        return msg_dict
    return None

def trigger_dag_run(context, dag_run_obj):
    # Obtenemos el mensaje procesado desde el XCom
    message = context['task_instance'].xcom_pull(task_ids='consume_from_kafka')
    if message:
        # Configuramos el payload del dag_run_obj con el mensaje JSON completo
        dag_run_obj.payload = message
        dag_run_obj.trigger_dag_id = 'sendmessage3'
        return dag_run_obj
    return None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_listener_dag',
    default_args=default_args,
    description='DAG that listens to Kafka and triggers other DAGs',
    schedule_interval='*/5 * * * *',  # Se ejecuta cada 5 minutos
    catchup=False,
)

consume_task = ConsumeFromTopicOperator(
    task_id='consume_from_kafka',
    kafka_config_id="kafka_connection",
    topics=['test1'],
    apply_function='dags.kafka_listener_dag.handle_message',
    dag=dag,
)

trigger_dag_task = TriggerDagRunOperator(
    task_id='trigger_dag_run',
    trigger_dag_id='sendmessage3',  # DAG a disparar
    python_callable=trigger_dag_run,
    provide_context=True,
    dag=dag,
)

consume_task >> trigger_dag_task
