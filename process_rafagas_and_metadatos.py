from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import uuid
import re
import json
from airflow.models import Variable
from dag_utils import get_minio_client, get_db_session, upload_to_minio_path

def extract_rafaga_parts(files_list):
    """
    Dada una lista de archivos con matrícula tipo PXXXXX1 o TXXXXX9, 
    agrupa y ordena las ráfagas encontradas por matrícula base.
    """
    rafagas = {}
    regex = re.compile(r'([PT]\w+)([19])$')  # Captura base y terminación 1 o 9
    
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
        else:
            # Archivo sin formato matrícula esperado, opcionalmente manejar
            pass
    return rafagas

def process_rafagas(**kwargs):
    dag_run = kwargs.get('dag_run')
    if not dag_run:
        print("No se encontró dag_run, abortando.")
        return

    conf = dag_run.conf or {}
    output = conf.get('output')
    output_json = conf.get('output_json')
    version = conf.get('version')

    if not output_json:
        print("No se recibió información de ráfaga en conf, abortando.")
        return

    print(f"Procesando ráfaga con versión: {version}")
    print(f"Output JSON recibido: {output_json}")

    # Adaptar la extracción de archivos según la estructura que envíes
    files_list = output_json.get('files_list', [])
    if not files_list:
        print("No hay lista de archivos para procesar ráfaga, terminando.")
        return

    minio_client = get_minio_client()
    session = get_db_session()

    rafagas = extract_rafaga_parts(files_list)

    for base, parts in rafagas.items():
        start_file = parts['start']
        end_file = parts['end']
        inter_files = parts['intermediate']

        if not start_file or not end_file:
            print(f"Ráfaga incompleta para base {base}, se omite")
            continue

        # Simulación de fechas, puedes extraer fechas reales si tienes metadatos
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=5)

        print(f"Procesando ráfaga {base}: inicio {start_time}, fin {end_time}, total archivos: {len(inter_files)+2}")

        mission_id = output_json.get('MissionID', 'MISION_DESCONOCIDA')
        all_files = [start_file] + inter_files + [end_file]

        for f in all_files:
            key = str(uuid.uuid4())
            try:
                print(f"Subiendo {f} a missions/{mission_id}/{key}")
                # Aquí tu código para subir a MinIO si tienes el path local
                # upload_to_minio_path('minio_conn', 'missions', f"{mission_id}/{key}/{f}", local_path)
            except Exception as e:
                print(f"Error subiendo {f}: {e}")

        try:
            insert_query = """
                INSERT INTO missions (mission_id, rafaga_base, start_time, end_time)
                VALUES (:mission_id, :base, :start, :end)
            """
            session.execute(insert_query, {'mission_id': mission_id, 'base': base, 'start': start_time, 'end': end_time})
            session.commit()
            print(f"Metadatos insertados para ráfaga {base}")
        except Exception as e:
            session.rollback()
            print(f"Error insertando en BD: {e}")

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