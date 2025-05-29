from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import uuid
import re
from dag_utils import get_minio_client, get_db_session, upload_to_minio_path

print("Starting process_rafagas_and_metadatos DAG")
print("Importing necessary modules...")
print("Modules imported successfully")

def extract_rafaga_parts(files_list):
    rafagas = {}
    regex = re.compile(r'([PT]\w+)([19])$')
    for file in files_list:
        m = regex.search(file)
        if m:
            base = m.group(1)
            suffix = m.group(2)
            if base not in rafagas:
                rafagas[base] = {'start': None, 'end': None, 'intermediate': []}
            if suffix == '1':
                rafagas[base]['start'] = file
            elif suffix == '9':
                rafagas[base]['end'] = file
            else:
                rafagas[base]['intermediate'].append(file)
    return rafagas

def process_rafagas(**kwargs):
    dag_run = kwargs.get('dag_run')
    if not dag_run:
        print("No dag_run found, aborting.")
        return

    conf = dag_run.conf or {}
    output_json = conf.get('output_json')
    print("Conf loaded successfully")
    print(f"Output JSON: {output_json}")    
    if not output_json:
        print("No output_json in conf, aborting.")
        return

    files_list = output_json.get('files_list', [])
    if not files_list:
        print("No files_list in output_json, finishing.")
        return

    session = get_db_session()
    rafagas = extract_rafaga_parts(files_list)
    mission_id = output_json.get('MissionID', 'MISION_DESCONOCIDA')
    piloto = output_json.get('PilotName')
    operador = output_json.get('OperatorName')

    for base, parts in rafagas.items():
        start_file = parts['start']
        end_file = parts['end']
        inter_files = parts['intermediate']

        if not start_file or not end_file:
            print(f"Incomplete rafaga for base {base}, skipping")
            continue

        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=5)

        # Query existing rafaga
        query_rafaga = """
        SELECT rafaga_id, minio_path FROM rafagas
        WHERE mission_id = :mission_id
          AND rafaga_base = :rafaga_base
          AND piloto = :piloto
          AND operador = :operador
          AND tsrange(start_time, end_time, '[)') && tsrange(:start_time, :end_time, '[)')
        LIMIT 1;
        """
        result = session.execute(query_rafaga, {
            'mission_id': mission_id,
            'rafaga_base': base,
            'piloto': piloto,
            'operador': operador,
            'start_time': start_time,
            'end_time': end_time
        }).fetchone()

        if result:
            rafaga_id = result['rafaga_id']
            minio_path = result['minio_path']
            print(f"Existing rafaga found with path {minio_path}")
        else:
            minio_path = f"{mission_id}/{str(uuid.uuid4())}"
            insert_rafaga = """
            INSERT INTO rafagas (mission_id, rafaga_base, piloto, operador, start_time, end_time, minio_path)
            VALUES (:mission_id, :rafaga_base, :piloto, :operador, :start_time, :end_time, :minio_path)
            RETURNING rafaga_id;
            """
            rafaga_id = session.execute(insert_rafaga, {
                'mission_id': mission_id,
                'rafaga_base': base,
                'piloto': piloto,
                'operador': operador,
                'start_time': start_time,
                'end_time': end_time,
                'minio_path': minio_path
            }).fetchone()[0]
            session.commit()
            print(f"New rafaga created with path {minio_path}")

        all_files = [start_file] + inter_files + [end_file]

        for f in all_files:
            key = str(uuid.uuid4())
            local_path = f"/tmp/{f}"  # Adjust to real local path
            try:
                upload_to_minio_path('minio_conn', 'missions', f"{minio_path}/{key}/{f}", local_path)
                print(f"File {f} uploaded to {minio_path}/{key}")
            except Exception as e:
                print(f"Error uploading {f}: {e}")

            # Insert metadata directly from output_json (assumed complete for each file)
            metadata = output_json

            tipo = metadata.get('tipo')
            captura_table = {
                0: "captura_rafaga_visible",
                1: "captura_rafaga_infrarroja",
                2: "captura_rafaga_multiespectral"
            }
            observation_table = {
                0: "observation_captura_rafaga_visible",
                1: "observation_captura_rafaga_infrarroja",
                2: "observation_captura_rafaga_multiespectral"
            }
            if tipo not in captura_table:
                print(f"Unknown type {tipo}, skipping metadata insert")
                continue

            insert_captura = f"""
            INSERT INTO {captura_table[tipo]} (
                valid_time, payload_id, multisim_id, ground_control_station_id, pc_embarcado_id,
                operator_name, pilot_name, sensor, platform
            ) VALUES (
                :valid_time, :payload_id, :multisim_id, :ground_control_station_id, :pc_embarcado_id,
                :operator_name, :pilot_name, :sensor, :platform
            ) RETURNING fid;
            """
            params_captura = {
                'valid_time': start_time,
                'payload_id': metadata.get('PayloadSN'),
                'multisim_id': metadata.get('MultisimSN'),
                'ground_control_station_id': metadata.get('GroundControlStationSN'),
                'pc_embarcado_id': metadata.get('PCEmbarcadoSN'),
                'operator_name': metadata.get('OperatorName'),
                'pilot_name': metadata.get('PilotName'),
                'sensor': metadata.get('Model'),
                'platform': metadata.get('AircraftNumberPlate'),
            }
            result = session.execute(insert_captura, params_captura)
            fid = result.fetchone()[0]

            # Static or generated shape (replace with your function)
            shape = "SRID=4326;POLYGON((-1 40, -1 41, 0 41, 0 40, -1 40))"

            insert_observation = f"""
            INSERT INTO {observation_table[tipo]} (
                procedure, sampled_feature, shape, result_time, phenomenon_time, identificador_rafaga
            ) VALUES (
                :procedure, :sampled_feature, :shape, :result_time, :phenomenon_time, :identificador_rafaga
            );
            """
            params_observation = {
                'procedure': metadata.get('Model'),
                'sampled_feature': metadata.get('MissionID'),
                'shape': shape,
                'result_time': start_time,
                'phenomenon_time': start_time,
                'identificador_rafaga': rafaga_id
            }
            session.execute(insert_observation, params_observation)
            session.commit()

            print(f"Metadata saved for file {f} in rafaga {rafaga_id}")

    session.close()

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
