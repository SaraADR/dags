import json
import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import text
from dag_utils import get_db_session, minio_api

def insert_rafaga_and_observation(**kwargs):
    print("[INFO] Iniciando procesamiento de ráfaga")
    conf = kwargs.get('dag_run').conf
    if not conf or 'output_json' not in conf:
        print("[ERROR] No se recibió 'output_json', abortando.")
        return

    output_json = conf['output_json']
    if isinstance(output_json, str):
        output_json = json.loads(output_json)
    print(f"[DEBUG] output_json recibido: {output_json}")

    minio_img_url = conf.get("RutaImagen", None)
    print(f"[INFO] URL de imagen recibida: {minio_img_url}")

    session = get_db_session()
    minio_base_url = minio_api()
    print(f"[INFO] Conexión a MinIO establecida con endpoint: {minio_base_url}")

    try:
        model = output_json.get("Model", "").lower()
        print(f"[INFO] Modelo detectado: {model}")

        if "infra" in model:
            tipo = "infrarroja"
        elif "multi" in model:
            tipo = "multiespectral"
        else:
            tipo = "visible"
        print(f"[INFO] Tipo de ráfaga determinado: {tipo}")

        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"
        tabla_temporal_subsample = f"observacion_aerea.observation_captura_rafaga_{tipo}_temporal_subsample"
        print(f"[INFO] Tablas seleccionadas: {tabla_captura}, {tabla_observacion}, {tabla_temporal_subsample}")

        rafaga_base = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")
        print(f"[INFO] Identificador ráfaga: {rafaga_base}, MissionID: {mission_id}")

        # Construir SQL y params según tipo (igual que antes)
        # Aquí solo muestro el visible para acortar, incluye los otros tipos en tu código original
        if tipo == "visible":
            exposure_time = output_json.get("exposureTime", None)
            insert_rafaga_sql = f"""
                INSERT INTO {tabla_captura} (
                    valid_time, payload_id, multisim_id, ground_control_station_id,
                    pc_embarcado_id, operator_name, pilot_name, sensor, platform,
                    exposuretime
                ) VALUES (
                    tsrange(now()::timestamp, (now() + interval '1 minute')::timestamp),
                    :payload_id, :multisim_id, :ground_control_station_id,
                    :pc_embarcado_id, :operator_name, :pilot_name, :sensor, :platform,
                    :exposure_time
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
                'exposure_time': exposure_time
            }
        # ... agrega aquí lógica para otros tipos (infra y multispectral) igual que tu código original

        print(f"[INFO] Ejecutando inserción en tabla captura: {tabla_captura}")
        result = session.execute(text(insert_rafaga_sql), params)
        fid_captura = result.scalar()
        print(f"[INFO] Insertado en {tabla_captura} con fid: {fid_captura}")

        valid_time_start = datetime.utcnow()
        valid_time_end = valid_time_start + timedelta(minutes=1)

        lat_str = output_json.get("GPSLatitude", "0").split()[0]
        lon_str = output_json.get("GPSLongitude", "0").split()[0]
        try:
            lat = float(lat_str)
            lon = float(lon_str)
            offset = 0.0001
            shape_wkt = (
                f"POLYGON(({lon - offset} {lat - offset}, "
                f"{lon - offset} {lat + offset}, "
                f"{lon + offset} {lat + offset}, "
                f"{lon + offset} {lat - offset}, "
                f"{lon - offset} {lat - offset}))"
            )
            print(f"[INFO] Geometría WKT generada: {shape_wkt}")
        except Exception as e:
            shape_wkt = "POLYGON((0 0,0 0,0 0,0 0,0 0))"
            print(f"[ERROR] Error al parsear coordenadas GPS: {e}. Usando polígono 0")

        insert_obs_sql = f"""
            INSERT INTO {tabla_observacion} (
                procedure, sampled_feature, shape, result_time, phenomenon_time, identificador_rafaga, temporal_subsamples
            ) VALUES (
                :procedure, :sampled_feature, ST_GeomFromText(:shape, 4326), :result_time,
                tsrange(:valid_time_start, :valid_time_end),
                :identificador_rafaga, '{{}}'
            )
            RETURNING fid
        """
        obs_params = {
            "procedure": int(output_json.get("SensorID", 0)),
            "sampled_feature": mission_id,
            "shape": shape_wkt,
            "result_time": valid_time_start,
            "valid_time_start": valid_time_start,
            "valid_time_end": valid_time_end,
            "identificador_rafaga": rafaga_base
        }
        print(f"[INFO] Ejecutando inserción en tabla observación: {tabla_observacion}")
        result_obs = session.execute(text(insert_obs_sql), obs_params)
        fid_observacion = result_obs.scalar()
        print(f"[INFO] Insertada observación en {tabla_observacion} con fid: {fid_observacion}")

        # Aquí NO subimos imagen porque ya está subida; usamos URL que viene en minio_img_url
        if minio_img_url:
            insert_subsample_sql = f"""
                INSERT INTO {tabla_temporal_subsample} (
                    phenomenon_time, image_url
                ) VALUES (
                    :phenomenon_time, :image_url
                )
            """
            subsample_params = {
                "phenomenon_time": valid_time_start,
                "image_url": minio_img_url
            }
            print(f"[INFO] Insertando subsample temporal en tabla: {tabla_temporal_subsample}")
            session.execute(text(insert_subsample_sql), subsample_params)
            print(f"[INFO] Insertado subsample temporal en {tabla_temporal_subsample}")
        else:
            print("[INFO] No se encontró URL de imagen para subsample temporal, se omite inserción.")

        session.commit()
        print("[INFO] Transacción confirmada en base de datos")

    except Exception as e:
        session.rollback()
        print(f"[ERROR] Error en inserción a BD: {e}")
    finally:
        session.close()
        print("[INFO] Sesión de base de datos cerrada")


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
