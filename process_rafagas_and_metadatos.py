from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import text
from dag_utils import get_db_session
import json
from datetime import datetime


def insert_rafaga_and_observation(**kwargs):
    conf = kwargs.get('dag_run').conf
    if not conf or 'output_json' not in conf:
        print("No se recibió 'output_json', abortando.")
        return
    
    output_json = conf['output_json']
    if isinstance(output_json, str):
        output_json = json.loads(output_json)
    
    print(f"Procesando ráfaga con Identificador: {output_json.get('IdentificadorRafaga')}")
    
    session = get_db_session()
    
    try:
        model = output_json.get("Model", "").lower()
        if "infra" in model:
            tipo = "infrarroja"
        elif "multi" in model:
            tipo = "multiespectral"
        else:
            tipo = "visible"

        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"

        rafaga_base = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")

        print(f"Tipo detectado: {tipo}")
        print(f"Tabla captura: {tabla_captura}")
        print(f"Tabla observación: {tabla_observacion}")

        insert_rafaga_sql = f"""
            INSERT INTO {tabla_captura} (
                valid_time, payload_id, multisim_id, ground_control_station_id,
                pc_embarcado_id, operator_name, pilot_name, sensor, platform
            ) VALUES (
                tsrange(now()::timestamp, (now() + interval '1 minute')::timestamp), 
                :payload_id, :multisim_id, :ground_control_station_id,
                :pc_embarcado_id, :operator_name, :pilot_name, :sensor, :platform
            ) RETURNING fid
        """


        params = {
            'payload_id': output_json.get('PayloadSN'),
            'multisim_id': output_json.get('MultisimSN'),
            'ground_control_station_id': output_json.get('GroundControlStationSN'),
            'pc_embarcado_id': output_json.get('PCEmbarcadoSN'),
            'operator_name': output_json.get('OperatorName'),
            'pilot_name': output_json.get('PilotName'),
            'sensor': output_json.get('Model'),
            'platform': output_json.get('AircraftNumberPlate'),
        }

        result = session.execute(text(insert_rafaga_sql), params)
        fid = result.scalar()
        print(f"Insertado en {tabla_captura} con fid: {fid}")

        lat_str = output_json.get("GPSLatitude", "0").split()[0]
        lon_str = output_json.get("GPSLongitude", "0").split()[0]
        try:
            lat = float(lat_str)
            lon = float(lon_str)
            shape_wkt = f"POINT({lon} {lat})"
        except Exception:
            shape_wkt = "POINT(0 0)"
            print("Error al parsear coordenadas GPS, usando POINT(0 0)")

        insert_obs_sql = f"""
            INSERT INTO {tabla_observacion} (
                procedure, sampled_feature, shape, result_time, phenomenon_time, identificador_rafaga, temporal_subsamples
            ) VALUES (
                :procedure, :sampled_feature, ST_GeomFromText(:shape, 4326), now(), now(), :identificador_rafaga, '{{}}'
            )
        """

        obs_params = {
            "procedure": int(output_json.get("SensorID", 0)),
            "sampled_feature": mission_id,
            "shape": shape_wkt,
            "identificador_rafaga": rafaga_base
        }

        session.execute(text(insert_obs_sql), obs_params)
        session.commit()
        print(f"Insertada observación en {tabla_observacion}")

    except Exception as e:
        session.rollback()
        print(f"Error en inserción a BD: {e}")
    finally:
        session.close()


default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'process_rafagas_and_metadatos',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

process_task = PythonOperator(
    task_id='process_rafagas',
    python_callable=insert_rafaga_and_observation,
    provide_context=True,
    dag=dag,
)

process_task
