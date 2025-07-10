from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import csv
import base64
import tempfile
import os
import paramiko
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from dag_utils import get_minio_client, get_db_session, minio_api, obtener_id_mision
from pytz import timezone
from utils.insert_end_of_execution import end_of_flow_task
from utils.callback_utils import task_failure_callback

madrid_tz = timezone('Europe/Madrid')
now = datetime.now(madrid_tz)

def execute_algorithm_remote(**context):
    print("Inicio de ejecución del algoritmo remoto")

    message = context['dag_run'].conf
    print("Datos recibidos desde el frontend:")
    print(json.dumps(message, indent=2))

    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str) if isinstance(input_data_str, str) else input_data_str
    print("Contenido de input_data:")
    print(json.dumps(input_data, indent=2))

    if "reserve" in input_data and "model" not in input_data["reserve"]:
        input_data["reserve"]["model"] = None

    assignment_id = input_data.get("assignmentId")

    if not assignment_id:
        raise ValueError("[ERROR] assignmentId no está presente en input_data.")
    context['ti'].xcom_push(key='assignment_id', value=assignment_id)

    if "fires" in input_data:
        for fire in input_data["fires"]:
            fire["level"] = 3

    user = message['message']['from_user']
    context['ti'].xcom_push(key='user', value=user)

    bastion_conn = BaseHook.get_connection("ssh_avincis_2")
    bastion_host = bastion_conn.host
    bastion_user = bastion_conn.login

    internal_conn = BaseHook.get_connection("ssh_citmaga_password")
    internal_user = internal_conn.login
    internal_password = internal_conn.password

    ssh_key = Variable.get("ssh_avincis_p-2")  # TODO sin decodificar
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as key_file:
        key_file.write(ssh_key)
        key_file_path = key_file.name
    os.chmod(key_file_path, 0o600)

    try:
        print("Estableciendo conexión SSH con bastión")
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=bastion_host, username=bastion_user, key_filename=key_file_path)

        jump_channel = bastion.get_transport().open_channel(
            "direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0)
        )

        print("Conectando al servidor interno")
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_client.connect(
            hostname="10.38.9.6",
            username=internal_user,
            password=internal_password,
            sock=jump_channel,
            look_for_keys=False,
            allow_agent=False
        )

        sftp = target_client.open_sftp()

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        execution_folder = f"EJECUCION_{assignment_id}_{timestamp}"
        context['ti'].xcom_push(key='execution_folder', value=execution_folder)
        base_path = f"/algoritms/executions/{execution_folder}"
        input_dir = f"{base_path}/input"
        output_dir = f"{base_path}/output"
        input_file = f"{input_dir}/input.json"
        output_file = f"{output_dir}/output.json"

        for path in [base_path, input_dir, output_dir]:
            try:
                sftp.stat(path)
            except FileNotFoundError:
                sftp.mkdir(path)
                print(f"Directorio creado: {path}")

        with sftp.file(input_file, 'w') as remote_file:
            remote_file.write(json.dumps(input_data, indent=2))
            print("Archivo input.json subido al servidor")
            print(json.dumps(input_data, indent=2))

        sftp.close()

        cmd = (
            f'python3 /algoritms/algoritmo-asignacion-aeronaves-objetivo-5/call_aircraft_dispatch.py '
            f'{input_file} {output_file}'
        )
        print("Ejecutando el algoritmo remoto")
        stdin, stdout, stderr = target_client.exec_command(cmd)
        print(stdout.read().decode())
        print(stderr.read().decode())

        # Cambiar permisos del output.json
        chmod_cmd = f'chmod 644 {output_file}'
        print(f"Cambiando permisos del archivo output.json con: {chmod_cmd}")
        stdin, stdout, stderr = target_client.exec_command(chmod_cmd)
        print(stdout.read().decode())
        print(stderr.read().decode())

        target_client.close()
        bastion.close()
        print("Conexión SSH finalizada")
        

    finally:
        os.remove(key_file_path)
        print("Archivo de clave SSH temporal eliminado")


def process_output_and_notify(**context):
    user = context['ti'].xcom_pull(key='user')
    execution_folder = context['ti'].xcom_pull(key='execution_folder')
    assignment_id = context['ti'].xcom_pull(key='assignment_id')
    print(f"[INFO] ID de asignación: {assignment_id}")
    print(f"[INFO] Procesando output del algoritmo desde carpeta: {execution_folder}")

    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname, username = ssh_conn.host, ssh_conn.login
    ssh_key_decoded = Variable.get("ssh_avincis_p-2")

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_key_path = temp_file.name
    os.chmod(temp_key_path, 0o600)

    local_json_path = f"/tmp/{assignment_id}_output.json"

    try:
        print("[INFO] Conectando a través del bastión SSH...")
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(hostname=hostname, username=username, key_filename=temp_key_path)

        jump = bastion.get_transport().open_channel("direct-tcpip", dest_addr=("10.38.9.6", 22), src_addr=("127.0.0.1", 0))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname="10.38.9.6", username="airflow-executor", sock=jump, key_filename=temp_key_path)

        sftp = client.open_sftp()
        base_path = "/algoritms/executions"
        folders = sftp.listdir(base_path)

        matching_folders = []
        for folder in sorted(folders, reverse=True):
            if folder.startswith(f"EJECUCION_{assignment_id}_"):
                candidate = f"{base_path}/{folder}/output/output.json"
                try:
                    sftp.stat(candidate)
                    matching_folders.append(candidate)
                except FileNotFoundError:
                    continue

        if not matching_folders:
            raise FileNotFoundError(f"[ERROR] No se encontró ningún output.json para assignment_id={assignment_id}")

        output_path = matching_folders[0]
        sftp.get(output_path, local_json_path)

        sftp.close()
        client.close()
        bastion.close()
    finally:
        os.remove(temp_key_path)

    print("[INFO] Leyendo output.json descargado")
    with open(local_json_path, 'r') as f:
        output_data = json.load(f)

    # Añadir missionId a cada assignment
    assignments = output_data.get("assignments", [])
    for assignment in assignments:
        fire_id = assignment.get("id")
        mission_id_local = obtener_id_mision(fire_id) if fire_id else None
        assignment["missionId"] = mission_id_local

    output_data["assignments"] = assignments

    fire_id_main = assignments[0]["id"] if assignments else None
    mission_id = obtener_id_mision(fire_id_main) if fire_id_main else None

    print("[DEBUG] output_data final antes de guardar y subir:")
    print(json.dumps(output_data, indent=2, ensure_ascii=False))

    # Guardar JSON actualizado antes de subir
    with open(local_json_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Añadido missionId a {len(assignments)} asignaciones")

    # Generar CSV
    csv_filename = f"{assignment_id}.csv"
    csv_local_path = f"/tmp/{csv_filename}"
    with open(csv_local_path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(["fire_id", "aircrafts"])
        for row in assignments:
            fire = row.get("id")
            aircrafts = ", ".join(row.get("vehicles", []))
            writer.writerow([fire, aircrafts])
    print(f"[INFO] CSV generado en {csv_local_path}")

    # Subir a MinIO
    s3_client = get_minio_client()
    bucket = "tmp"
    folder = "algorithm_aircraft_planificator_outputs"
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    json_key = f"{folder}/jsons/{assignment_id}_{timestamp}.json"
    csv_key = f"{folder}/outputs/{assignment_id}_{timestamp}.csv"

    s3_client.upload_file(local_json_path, bucket, json_key)
    s3_client.upload_file(csv_local_path, bucket, csv_key)
    print(f"[INFO] Archivos subidos a MinIO: {json_key}, {csv_key}")

    # Notificación
    base_url = minio_api()
    json_url = f"{base_url}/{bucket}/{json_key}"
    csv_url = f"{base_url}/{bucket}/{csv_key}"

    session = get_db_session()
    now_utc = datetime.utcnow()
    result = session.execute(text("""
        INSERT INTO public.notifications (destination, "data", "date", status)
        VALUES ('ignis', '{}', :date, NULL)
        RETURNING id
    """), {'date': now_utc})
    job_id = result.scalar()

    payload = {
        "to": user,
        "actions": [
            {
                "type": "loadTable",
                "data": {
                    "url": csv_url,
                    "button": {
                        "key": "openPlanner",
                        "data": json_url
                    },
                    "title": "Recomendación de aeronaves",
                    "missionId": mission_id
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
    print(f"[INFO] Notificación insertada en DB y enviada a {user}")

    # Guardar en histórico de resultados
    try:
        print("[INFO] Guardando resultado en algoritmos.algoritmo_aircraft_recomendador...")
        session = get_db_session()
        session.execute(text("""
            INSERT INTO algoritmos.algoritmo_aircraft_recomendador (sampled_feature, result_time, phenomenon_time, input_data, output_data)
            VALUES (:sampled_feature, :result_time, :phenomenon_time, :input_data, :output_data)
        """), {
            "sampled_feature": fire_id_main if fire_id_main is not None else 0,
            "result_time": datetime.now(madrid_tz),
            "phenomenon_time": datetime.now(madrid_tz),
            "input_data": json.dumps({}, ensure_ascii=False),
            "output_data": json.dumps(output_data, ensure_ascii=False)
        })
        session.commit()
        print("[INFO] Resultado insertado correctamente en la tabla.")
    except Exception as e:
        session.rollback()
        print(f"[ERROR] Error al guardar en la base de datos: {e}")
        raise
    finally:
        session.close()
        print("[INFO] Sesión SQL cerrada correctamente")


default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': task_failure_callback
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