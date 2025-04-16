from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import csv
import base64
import tempfile
import os
from io import BytesIO
import paramiko
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from dag_utils import get_minio_client, get_db_session


def execute_algorithm_remote(**context):
    print("[INFO] Inicio de ejecución del algoritmo remoto")

    message = context['dag_run'].conf
    print("[INFO] Datos recibidos desde el frontend:")
    print(json.dumps(message, indent=2))

    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
    print("[INFO] Contenido de input_data:")
    print(json.dumps(input_data, indent=2))

    user = message['message']['from_user']
    context['ti'].xcom_push(key='user', value=user)

    assignment_id = input_data.get("assignmentId")
    print(f"[INFO] assignment_id: {assignment_id}")


def process_output_and_notify(**context):

    raw_input_data = context['dag_run'].conf['message']['input_data']
    input_data = json.loads(raw_input_data) if isinstance(raw_input_data, str) else raw_input_data
    assignment_id = input_data['assignmentId']


    user = context['ti'].xcom_pull(key='user')
    print(f"[INFO] Procesando output y notificando al frontend")
    print(f"[INFO] assignment_id: {assignment_id}")

    # 1. Descargar JSON desde MinIO
    s3_client = get_minio_client()
    bucket = "tmp"
    test_json_key = "algorithm_aircraft_planificator_outputs/historic/input_test2.json"
    print(f"[INFO] Descargando JSON de prueba desde MinIO: {test_json_key}")
    
    response = s3_client.get_object(
        Bucket=bucket,
        Key=test_json_key
    )
    output_data = json.load(response["Body"])
    print("[INFO] JSON descargado desde MinIO")

    # 2. Generar CSV local
    csv_data = output_data.get("resourcePlanningResult", [])
    csv_filename = f"{assignment_id}.csv"
    csv_local_path = f"/tmp/{csv_filename}"
    with open(csv_local_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["since", "until", "aircrafts"])
        writer.writeheader()
        for row in csv_data:
            writer.writerow({
                "since": row.get("since"),
                "until": row.get("until"),
                "aircrafts": ", ".join(row.get("aircrafts", []))
            })

    # 3. Subir JSON y CSV a MinIO
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    json_key = f"algorithm_aircraft_planificator_outputs/jsons/{assignment_id}_{timestamp}.json"
    csv_key = f"algorithm_aircraft_planificator_outputs/outputs/{assignment_id}_{timestamp}.csv"

    s3_client.put_object(
        Bucket=bucket,
        Key=json_key,
        Body=BytesIO(json.dumps(output_data).encode("utf-8")),
        ContentLength=len(json.dumps(output_data).encode("utf-8"))
    )

    s3_client.upload_file(csv_local_path, bucket, csv_key)

    json_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{json_key}"
    csv_url = f"https://minio.avincis.cuatrodigital.com/{bucket}/{csv_key}"
    print(f"[INFO] Archivos subidos a MinIO: \n- JSON: {json_url}\n- CSV: {csv_url}")

    # 4. Insertar notificación en BD
    session = get_db_session()
    now_utc = datetime.utcnow()
    result = session.execute(text("""
        INSERT INTO public.notifications (destination, "data", "date", status)
        VALUES ('ignis', '{}', :date, NULL)
        RETURNING id
    """), {'date': now_utc})
    job_id = result.scalar()
    print(f"[INFO] Notificación registrada con ID: {job_id}")

    # 5. Construir payload de notificación
    payload = {
        "to": user,
        "actions": [
            {
                "type": "loadTable",
                "data": {
                    "url": csv_url
                }
            },
            {
                "type": "paintCSV",
                "data": {
                    "url": csv_url,
                    "action": {
                        "key": "openPlanner",
                        "data": json_url
                    },
                    "title": "Abrir planner"
                }
            },
            {
                "type": "notify",
                "data": {
                    "message": f"Resultados del algoritmo disponibles. ID: {job_id}"
                }
            }
        ]
    }

    session.execute(text("""
        UPDATE public.notifications SET data = :data WHERE id = :id
    """), {"data": json.dumps(payload, ensure_ascii=False), "id": job_id})
    session.commit()
    session.close()
    print("[INFO] Notificación actualizada y enviada")



default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_aircraft_recommendation',
    default_args=default_args,
    description='Ejecuta algoritmo de recomendación en servidor Avincis',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

execute_task = PythonOperator(
    task_id='execute_assignation_algorithm',
    python_callable=execute_algorithm_remote,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_output_and_notify',
    python_callable=process_output_and_notify,
    dag=dag
)

execute_task >> process_task
