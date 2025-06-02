import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from dag_utils import get_db_session, minio_api


def insert_rafaga_and_observation(**kwargs):
    print("[INFO] Iniciando procesamiento de ráfaga")

    # Configuración pasada desde el DAG que dispara este
    conf = kwargs.get('dag_run').conf
    if not conf or 'output_json' not in conf:
        print("[ERROR] No se recibió 'output_json', abortando.")
        return

    # Parsear configuración
    output_json = conf['output_json']
    if isinstance(output_json, str):
        output_json = json.loads(output_json)

    print(f"[DEBUG] output_json recibido: {json.dumps(output_json, indent=2)}")

    # URL de imagen (si se pasó)
    minio_img_url = conf.get("RutaImagen", None)
    print(f"[INFO] URL de imagen recibida: {minio_img_url}")

    # Sesión de base de datos y conexión a MinIO
    session = get_db_session()
    minio_base_url = minio_api()
    print(f"[INFO] Conexión a MinIO establecida: {minio_base_url}")

    try:
        # Obtener el modelo para identificar el tipo
        model = output_json.get("Model", "").lower()
        if not model:
            print("[ERROR] El campo 'Model' está vacío o ausente.")
            return

        if "infra" in model:
            tipo = "infrarroja"
        elif "multi" in model:
            tipo = "multiespectral"
        else:
            tipo = "visible"

        print(f"[INFO] Tipo de ráfaga determinado: {tipo}")

        # Definir nombres de tablas
        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"
        tabla_temporal_subsample = f"{tabla_observacion}_temporal_subsample"
        print(f"[INFO] Tablas:\n  Captura: {tabla_captura}\n  Observación: {tabla_observacion}\n  Subsample: {tabla_temporal_subsample}")

        # Variables clave
        rafaga_base = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")

        print(f"[INFO] Identificador de ráfaga: {rafaga_base}")
        print(f"[INFO] Misión: {mission_id}")

        # ------------------------- INSERT EN CAPTURA -------------------------
        if tipo == "visible":
            exposure_time = output_json.get("exposureTime", None)
            insert_rafaga_sql = f"""
                INSERT INTO {tabla_captura} (
                    valid_time, payload_id, multisim_id, ground_control_station_id,
                    pc_embarcado_id, operator_name, pilot_name, sensor, platform,
                    exposuretime
                ) VALUES (
                    tsrange(now(), now() + interval '1 minute'),
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
        else:  # infrarroja o multiespectral
            insert_rafaga_sql = f"""
                INSERT INTO {tabla_captura} (
                    valid_time, payload_id, multisim_id, ground_control_station_id,
                    pc_embarcado_id, operator_name, pilot_name, sensor, platform
                ) VALUES (
                    tsrange(now(), now() + interval '1 minute'),
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

        print("[INFO] Ejecutando inserción en tabla de captura...")
        result = session.execute(text(insert_rafaga_sql), params)
        fid_captura = result.scalar()
        print(f"[OK] Insertado en {tabla_captura} con fid: {fid_captura}")

        # ------------------------- INSERT EN OBSERVACIÓN -------------------------
        valid_time_start = datetime.utcnow()
        valid_time_end = valid_time_start + timedelta(minutes=1)

        try:
            lat_str = output_json.get("GPSLatitude", "0").split()[0]
            lon_str = output_json.get("GPSLongitude", "0").split()[0]
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
            print(f"[INFO] Geometría generada: {shape_wkt}")
        except Exception as e:
            shape_wkt = "POLYGON((0 0,0 0,0 0,0 0,0 0))"
            print(f"[WARN] Error al parsear coordenadas: {e}. Usando polígono nulo.")

        insert_obs_sql = f"""
            INSERT INTO {tabla_observacion} (
                procedure, sampled_feature, shape, result_time, phenomenon_time, identificador_rafaga, temporal_subsamples
            ) VALUES (
                :procedure, :sampled_feature, ST_GeomFromText(:shape, 4326), :result_time,
                tsrange(:valid_time_start, :valid_time_end),
                :identificador_rafaga, '{{}}'
            ) RETURNING fid
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

        print(f"[INFO] Ejecutando inserción en {tabla_observacion}...")
        result_obs = session.execute(text(insert_obs_sql), obs_params)
        fid_observacion = result_obs.scalar()
        print(f"[OK] Insertada observación con fid: {fid_observacion}")

        # ------------------------- INSERT EN SUBSAMPLE TEMPORAL -------------------------
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
            session.execute(text(insert_subsample_sql), subsample_params)
            print(f"[OK] Subsample temporal insertado en {tabla_temporal_subsample}")
        else:
            print("[INFO] No se proporcionó imagen para subsample temporal.")

        session.commit()
        print("[SUCCESS] Todos los cambios confirmados.")

    except Exception as e:
        session.rollback()
        print(f"[ERROR] Error durante el procesamiento: {e}")
    finally:
        session.close()
        print("[INFO] Sesión de base de datos cerrada.")


# ------------------ DEFINICIÓN DEL DAG ------------------

default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'process_rafagas_and_metadatos',
    default_args=default_args,
    description='DAG que procesa ráfagas visibles, infrarrojas y multiespectrales',
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
