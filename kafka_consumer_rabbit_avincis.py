import json
import uuid
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta, timezone

# Configuración del DAG
default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_consumer_rabbit_avincis',
    default_args=default_args,
    description='DAG que consume eventos de RabbitMQ/Kafka y los redirige según el tipo de evento.',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1    
)

def process_and_upload_json(**kwargs):
    """ Procesa el mensaje Kafka y sube el JSON al servidor vía SSH. """
    ti = kwargs['ti']
    message = ti.xcom_pull(task_ids='consume_from_topic')  

    if not message:
        print("No se encontró mensaje en XCom")
        return None

    try:
        msg_json = json.loads(message)  
        event_name = msg_json.get("eventName", "")

        # Determinar el DAG objetivo
        target_dag = "algorithm_gifs_fire_prediction_post_process" if event_name == "GIFAlgorithmExecution" else "function_create_fire_from_rabbit"
        print(f"Redirigiendo evento a DAG: {target_dag}")

        ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

        try:
            with ssh_hook.get_conn() as ssh_client:
                sftp = ssh_client.open_sftp()

                # Guardamos el JSON en el servidor
                file_path = "/home/admin3/grandes-incendios-forestales/input_automatic.json"

                with sftp.file(file_path, "w") as json_file:
                    json.dump(msg_json, json_file, ensure_ascii=False, indent=4)

                print(f"Archivo JSON guardado en {file_path}")

                sftp.close()

        except Exception as e:
            print(f"Error en la conexión SSH: {e}")
            return None

        return {"file_path": file_path, "target_dag": target_dag}

    except json.JSONDecodeError as e:
        print(f"Error decodificando JSON: {e}")
    except Exception as e:
        print(f"Error en la ejecución: {e}")

def trigger_dag(**kwargs):
    """ Dispara el DAG objetivo con la ruta del JSON en el servidor. """
    ti = kwargs['ti']
    dag_info = ti.xcom_pull(task_ids='process_and_upload_json')

    if not dag_info:
        print("No se encontró información del DAG en XCom")
        return None

    file_path = dag_info["file_path"]
    target_dag = dag_info["target_dag"]

    trigger = TriggerDagRunOperator(
        task_id="trigger_dag_task",
        trigger_dag_id=target_dag,
        conf={"file_path": file_path},
        execution_date=datetime.now().replace(tzinfo=timezone.utc),
        dag=dag
    )

    trigger.execute(context=kwargs)  

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["rabbit_einforex"],
    apply_function=lambda msg, **kwargs: kwargs['ti'].xcom_push(key='message', value=msg.value().decode('utf-8')),
    commit_cadence="end_of_batch",
    dag=dag,
)

process_and_upload_json_task = PythonOperator(
    task_id="process_and_upload_json",
    python_callable=process_and_upload_json,
    provide_context=True,
    dag=dag,
)

trigger_dag_task = PythonOperator(
    task_id="trigger_dag_task",
    python_callable=trigger_dag,
    provide_context=True,
    dag=dag,
)

consume_from_topic >> process_and_upload_json_task >> trigger_dag_task
