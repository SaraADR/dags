from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
import json
from dag_utils import get_db_session, minio_api

def insert_rafaga_and_observation(FileName: str, **kwargs) -> str:
    print("\n[INFO] Iniciando procesamiento de ráfaga")

    conf = kwargs.get('dag_run').conf
    print(f"[DEBUG] Conf recibida: {conf}")
    
    if not conf or 'output_json' not in conf:
        print("[ERROR] No se recibió 'output_json', abortando ejecución.")
        return

    output_json = conf['output_json']
    if isinstance(output_json, str):
        output_json = json.loads(output_json)
    print(f"[DEBUG] output_json recibido:\n{json.dumps(output_json, indent=2)}")

    minio_img_url = conf.get("RutaImagen", None)
    print(f"[INFO] URL de imagen (MinIO): {minio_img_url}")

    session = get_db_session()
    print("[INFO] Sesión de base de datos abierta correctamente.")

    minio_base_url = minio_api()
    print(f"[INFO] Conexión a MinIO: {minio_base_url}")

    

    try:
        nombre = FileName.lower()
    
        if nombre.endswith("-ter.tiff"):
            print("[INFO] Ráfaga detectada como INFRARROJA por sufijo '-ter.tiff'")
            tipo = "infrarroja"
        elif "-mul-" in nombre or "-band_" in nombre:
            print("[INFO] Ráfaga detectada como MULTIESPECTRAL por sufijo '-mul-' o '-band_'")
            tipo = "multiespectral"
        elif "-harrier.tiff" in nombre or "-basler.tiff" in nombre:
            print("[INFO] Ráfaga detectada como VISIBLE por sufijo '-harrier.tiff' o '-basler.tiff'")
            tipo = "visible"
        else:
            print(f"[WARNING] No se pudo determinar tipo desde nombre: '{FileName}'")


        print(f"[INFO] Tipo de ráfaga determinado: {tipo}")

        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"
        tabla_temporal_subsample = f"{tabla_observacion}_temporal_subsample"
        tabla_imagen = f"observacion_aerea.observation_captura_imagen_{tipo}"

        print(f"[INFO] Tablas utilizadas:")
        print(f" - Captura: {tabla_captura}")
        print(f" - Observación: {tabla_observacion}")
        print(f" - Subsample temporal: {tabla_temporal_subsample}")
        print(f" - Imagen individual: {tabla_imagen}")

        rafaga_id = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")
        print(f"[INFO] Identificador de ráfaga: {rafaga_id}")
        print(f"[INFO] ID de misión: {mission_id}")

        # Insertar en tabla de captura
        print("[INFO] Preparando inserción en tabla de captura...")

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
        print(f"[DEBUG] Parámetros de inserción captura: {params}")
        result = session.execute(text(insert_rafaga_sql), params)
        captura_fid = result.fetchone()[0] if result.returns_rows else None
        print(f"[OK] Fila de captura insertada con fid: {captura_fid}")

        # Fechas y geometría
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
            print(f"[INFO] Geometría generada: {shape_wkt}")
        except Exception as e:
            shape_wkt = "POLYGON((0 0,0 0,0 0,0 0,0 0))"
            print(f"[WARN] Error generando geometría: {e}. Usando geometría nula.")

        # Inserción en observación ráfaga
        print("[INFO] Insertando en observación ráfaga...")
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
        print(f"[DEBUG] Parámetros observación: {obs_params}")
        session.execute(text(insert_obs_sql), obs_params)
        print("[OK] Observación de ráfaga insertada.")

        # Subsample temporal si hay URL
        if minio_img_url:
            print("[INFO] Insertando en subsample temporal...")
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
            print("[OK] Subsample temporal insertado.")
        else:
            print("[INFO] No se proporcionó URL de imagen para subsample temporal.")

        # Observación imagen individual
        print("[INFO] Insertando imagen individual...")
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
        print("[OK] Imagen individual insertada.")

        session.commit()
        print("[SUCCESS] Todos los datos se han guardado correctamente.")
    except Exception as e:
        session.rollback()
        print(f"[ERROR] Excepción durante el procesamiento: {e}")
    finally:
        session.close()
        print("[INFO] Sesión de base de datos cerrada.")

# DAG definition
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
