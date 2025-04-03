from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import boto3

# Lista de DAGs 
dag_ids_to_monitor = ['algorithm_dNBR_process_Type1', 'kafka_consumer_classify_files_and_trigger_dags', 'kafka_consumer_trigger_jobs', ]


def upload_logs_to_s3(dag_id, execution_date):
    log_file_path = f"/opt/airflow/logs/{dag_id}/{execution_date}.log"

    try:
        with open(log_file_path, "r") as log_file:
            logs = log_file.read()

        print(logs)

        # # Configurar conexión con S3
        # s3_client = boto3.client('s3')
        # bucket_name = "tu-bucket-s3"
        # s3_key = f"logs/{dag_id}/{execution_date}.txt"

        # s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=logs)
        # print(f"Logs de {dag_id} subidos exitosamente a {s3_key}")

    except Exception as e:
        print(f"Error al leer o subir logs para {dag_id}: {e}")



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'read_upload_logs',
    default_args=default_args,
    schedule_interval=None, 
)

# Lista de sensores y tareas de subida de logs
sensors = []
upload_tasks = []

for dag_id in dag_ids_to_monitor:
    # Sensor que espera la finalización de cada DAG
    sensor_task = ExternalTaskSensor(
        task_id=f'wait_for_{dag_id}',
        external_dag_id=dag_id,
        execution_delta=timedelta(minutes=1),
        mode='poke',
        dag=dag,
    )
    
    upload_task = PythonOperator(
        task_id=f'upload_logs_{dag_id}',
        python_callable=upload_logs_to_s3,
        op_args=[dag_id, '{{ ds }}'],
        dag=dag,
    )

    # Establecer dependencia: cuando el DAG finaliza, se suben sus logs
    sensor_task >> upload_task
    
    # Guardamos las tareas en listas por si queremos administrarlas dinámicamente
    sensors.append(sensor_task)
    upload_tasks.append(upload_task)
