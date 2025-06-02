from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
import json
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
    print(f"[DEBUG] output_json recibido: {json.dumps(output_json, indent=2)}")

    minio_img_url = conf.get("RutaImagen", None)
    print(f"[INFO] URL de imagen recibida: {minio_img_url}")

    session = get_db_session()
    minio_base_url = minio_api()
    print(f"[INFO] Conexión a MinIO establecida: {minio_base_url}")

    try:
        model = output_json.get("Model", "").lower()
        if not model:
            print("[ERROR] El campo 'Model' está vacío o ausente.")
            return

        # Determinar tipo
        if "infra" in model:
            tipo = "infrarroja"
        elif "multi" in model:
            tipo = "multiespectral"
        else:
            tipo = "visible"

        print(f"[INFO] Tipo de ráfaga determinado: {tipo}")

        # Nombres de tablas
        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"
        tabla_temporal_subsample = f"{tabla_observacion}_temporal_subsample"
        tabla_imagen = f"observacion_aerea.observation_captura_imagen_{tipo}"

        print(f"[INFO] Tablas:\n  Captura: {tabla_captura}\n  Observación: {tabla_observacion}\n  Imagen individual: {tabla_imagen}")

        rafaga_id = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")

        # INSERT EN CAPTURA (una vez por ráfaga)
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
        else:
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

        print("[INFO] Ejecutando inserción en tabla de captura...")
        session.execute(text(insert_rafaga_sql), params)

        # FECHAS Y GEOMETRÍA
        valid_time_start = datetime.utcnow()
        valid_time_end = valid_time_start + timedelta(minutes=1)

        try:
            lat = float(output_json.get("GPSLatitude", "0").split()[0])
            lon = float(output_json.get("GPSLongitude", "0").split()[0])
            offset = 0.0001
            shape_wkt = (
                f"POLYGON(({lon - offset} {lat - offset}, "
                f"{lon - offset} {lat + offset}, "
                f"{lon + offset} {lat + offset}, "
                f"{lon + offset} {lat - offset}, "
                f"{lon - offset} {lat - offset}))"
            )
        except Exception as e:
            print(f"[WARN] Coordenadas inválidas: {e}")
            shape_wkt = "POLYGON((0 0,0 0,0 0,0 0,0 0))"

        # INSERT EN OBSERVACIÓN RÁFAGA (una por imagen)
        insert_obs_sql = f"""
            INSERT INTO {tabla_observacion} (
                procedure, sampled_feature, shape, result_time, phenomenon_time, identificador_rafaga, temporal_subsamples
            ) VALUES (
                :procedure, :sampled_feature, ST_GeomFromText(:shape, 4326), :result_time,
                tsrange(:valid_time_start, :valid_time_end), :identificador_rafaga, '{{}}'
            )
        """
        obs_params = {
            "procedure": int(output_json.get("SensorID", 0)),
            "sampled_feature": mission_id,
            "shape": shape_wkt,
            "result_time": valid_time_start,
            "valid_time_start": valid_time_start,
            "valid_time_end": valid_time_end,
            "identificador_rafaga": rafaga_id
        }

        print("[INFO] Ejecutando inserción en observación ráfaga...")
        session.execute(text(insert_obs_sql), obs_params)

        # INSERT EN SUBSAMPLE TEMPORAL
        if minio_img_url:
            insert_subsample_sql = f"""
                INSERT INTO {tabla_temporal_subsample} (
                    phenomenon_time, image_url
                ) VALUES (
                    :phenomenon_time, :image_url
                )
            """
            session.execute(text(insert_subsample_sql), {
                "phenomenon_time": valid_time_start,
                "image_url": minio_img_url
            })
            print("[INFO] Subsample temporal insertado.")

        # INSERT EN OBSERVACIÓN DE IMAGEN INDIVIDUAL
        output_json["ReadedFromVersion"] = conf.get("version", "desconocida")
        insert_img_sql = f"""
            INSERT INTO {tabla_imagen} (
                shape, sampled_feature, procedure, result_time, phenomenon_time, imagen
            ) VALUES (
                ST_GeomFromText(:shape, 4326),
                :sampled_feature,
                :procedure,
                :result_time,
                :phenomenon_time,
                :imagen
            )
        """
        session.execute(text(insert_img_sql), {
            "shape": shape_wkt,
            "sampled_feature": mission_id,
            "procedure": int(output_json.get("SensorID", 0)),
            "result_time": valid_time_start,
            "phenomenon_time": valid_time_start,
            "imagen": json.dumps(output_json, ensure_ascii=False)
        })
        print("[INFO] Imagen individual insertada.")

        session.commit()
        print("[SUCCESS] Todos los cambios confirmados.")
    except Exception as e:
        session.rollback()
        print(f"[ERROR] Error durante el procesamiento: {e}")
    finally:
        session.close()
        print("[INFO] Sesión de base de datos cerrada.")


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
    description='Guarda metadatos generales de ráfaga + imagen individual',
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
