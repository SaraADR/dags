from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json

from dag_utils import get_minio_client, get_db_session, upload_to_minio_path
import uuid

def process_rafagas(**kwargs):
    dag_run = kwargs.get('dag_run')
    if not dag_run:
        print("No dag_run found, aborting.")
        return

    conf = dag_run.conf or {}
    output_json = conf.get('output_json')
    output = conf.get('output')
    version = conf.get('version')

    print("=== process_rafagas START ===")
    print(f"Version recibida: {version}")
    print(f"Output completo: {output}")
    print("Output_json completo:")
    print(json.dumps(output_json, indent=4, ensure_ascii=False))

    identificador_rafaga = output_json.get("IdentificadorRafaga")
    if not identificador_rafaga:
        print("No se encontró IdentificadorRafaga, abortando.")
        return

    # Abrir sesión a BD y cliente MinIO
    session = get_db_session()
    minio_client = get_minio_client()

    # Ejemplo simple: subimos el archivo a MinIO en carpeta basada en misión y ráfaga
    mission_id = str(output_json.get("MissionID", "sin_mision"))
    rafaga_id = identificador_rafaga
    file_name = output_json.get("FileName", f"{uuid.uuid4()}.tiff")

    minio_path = f"missions/{mission_id}/{rafaga_id}/{file_name}"

    try:
        # Aquí asumirías que tienes el archivo local, ejemplo temporal:
        local_file_path = f"/tmp/{file_name}"  # Ajustar según tu entorno real
        upload_to_minio_path('minio_conn', 'missions', minio_path, local_file_path)
        print(f"Archivo subido a MinIO en {minio_path}")
    except Exception as e:
        print(f"Error subiendo archivo a MinIO: {e}")

    # Aquí deberías insertar en la tabla de ráfagas y en tablas específicas según tipo
    # Ejemplo básico de insert, adapta a tus tablas y campos

    try:
        # Consulta si ya existe esta ráfaga para la misión
        query_rafaga = """
        SELECT rafaga_id FROM rafagas
        WHERE rafaga_base = :rafaga_base AND mission_id = :mission_id
        LIMIT 1;
        """
        result = session.execute(query_rafaga, {'rafaga_base': rafaga_id, 'mission_id': mission_id}).fetchone()

        if result:
            print(f"Ráfaga existente: {result['rafaga_id']}")
            rafaga_db_id = result['rafaga_id']
        else:
            insert_rafaga = """
            INSERT INTO rafagas (mission_id, rafaga_base, start_time, end_time, minio_path)
            VALUES (:mission_id, :rafaga_base, now(), now(), :minio_path)
            RETURNING rafaga_id;
            """
            result = session.execute(insert_rafaga, {
                'mission_id': mission_id,
                'rafaga_base': rafaga_id,
                'minio_path': minio_path
            })
            rafaga_db_id = result.fetchone()[0]
            session.commit()
            print(f"Ráfaga creada con id: {rafaga_db_id}")

        # Insertar en tabla específica según tipo (visible, infrarroja, multiespectral)
        tipo = output_json.get('tipo', 0)  # Ajusta según cómo venga el tipo
        tabla_captura = {
            0: "observacion_aerea.captura_imagen_visible",
            1: "observacion_aerea.captura_imagen_infrarroja",
            2: "observacion_aerea.captura_imagen_multiespectral"
        }.get(tipo, "observacion_aerea.captura_imagen_visible")

        insert_captura = f"""
        INSERT INTO {tabla_captura} (fid, valid_time, payload_id, multisim_id, ground_control_station_id,
                                    pc_embarcado_id, operator_name, pilot_name, sensor, platform)
        VALUES (:fid, now(), :payload_id, :multisim_id, :ground_control_station_id,
                :pc_embarcado_id, :operator_name, :pilot_name, :sensor, :platform)
        RETURNING fid;
        """
        params = {
            'fid': int(output_json.get('SensorID', 0)),
            'payload_id': output_json.get('PayloadSN'),
            'multisim_id': output_json.get('MultisimSN'),
            'ground_control_station_id': output_json.get('GroundControlStationSN'),
            'pc_embarcado_id': output_json.get('PCEmbarcadoSN'),
            'operator_name': output_json.get('OperatorName'),
            'pilot_name': output_json.get('PilotName'),
            'sensor': output_json.get('Model'),
            'platform': output_json.get('AircraftNumberPlate'),
        }
        result = session.execute(insert_captura, params)
        fid = result.fetchone()[0]
        session.commit()
        print(f"Captura insertada con fid: {fid}")

    except Exception as e:
        session.rollback()
        print(f"Error en inserción a BD: {e}")
    finally:
        session.close()

    print("=== process_rafagas END ===")


default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'process_rafagas_and_metadatos',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

t_process_rafagas = PythonOperator(
    task_id='process_rafagas',
    python_callable=process_rafagas,
    provide_context=True,
    dag=dag,
)

t_process_rafagas
