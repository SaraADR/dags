import json
import uuid
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta, timezone


def consumer_function(message, **kwargs):
    """ Procesa el mensaje recibido desde Kafka y lo envía al DAG correspondiente. """
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print("Mensaje consumido:")
        print(f"{msg_value}")

        if msg_value:
            process_message(msg_value, kwargs)
        else:
            print("Mensaje vacío recibido.")
            return None
    else:
        print("Mensaje vacío recibido.")
        return None

def process_message(message, kwargs):
    """ Filtra y redirige el mensaje al DAG correcto y envía el JSON al servidor vía SSH. """
    try:
        msg_json = json.loads(message)  # Convertir el mensaje a JSON
        event_name = msg_json.get("eventName", "")

        # Definir DAG objetivo
        target_dag = "algorithm_gifs_fire_prediction_post_process" if event_name == "GIFAlgorithmExecution" else "function_create_fire_from_rabbit"
        print(f"Redirigiendo evento a DAG: {target_dag}")

        # ID único para el proceso
        unique_id = str(uuid.uuid4())

        # onfigurar conexión SSH usando Airflow
        ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

        try:
            with ssh_hook.get_conn() as ssh_client:
                sftp = ssh_client.open_sftp()

                # Obtener la ruta del servidor desde Airflow
                file_path = "/home/admin3/grandes-incendios-forestales/input_automatic.json"

                # Subir el JSON al servidor
                with sftp.file(file_path, "w") as json_file:
                    json.dump(msg_json, json_file, ensure_ascii=False, indent=4)

                print(f"Archivo JSON guardado en {file_path}")

                sftp.close()

        except Exception as e:
            print(f"Error en la conexión SSH: {e}")
            return
    
        # Disparar el DAG correspondiente con la referencia al archivo JSON en el servidor
        trigger_dag_run = TriggerDagRunOperator(
            task_id=unique_id,
            trigger_dag_id=target_dag,
            conf={"file_path": file_path},  # Pasamos la ruta en el servidor
            execution_date=datetime.now().replace(tzinfo=timezone.utc),
            dag=dag
        )

        trigger_dag_run.execute(context=kwargs)

    except json.JSONDecodeError as e:
        print(f"Error decodificando JSON: {e}")
    except Exception as e:
        print(f"Error en la ejecución: {e}")

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

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["rabbit_einforex"],
    apply_function=consumer_function,
    commit_cadence="end_of_batch",
    dag=dag,
)

consume_from_topic
