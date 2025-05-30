import json
import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import text
from dag_utils import get_db_session, get_minio_client, minio_api, upload_to_minio


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

    visible_img = conf.get("visible_image", None)
    thermic_img = conf.get("thermic_image", None)
    multispectral_img = conf.get("multispectral_image", None)
    print(f"[DEBUG] visible_img presente: {visible_img is not None}")
    print(f"[DEBUG] thermic_img presente: {thermic_img is not None}")
    print(f"[DEBUG] multispectral_img presente: {multispectral_img is not None}")

    session = get_db_session()
    s3_client = get_minio_client()
    bucket = 'tmp'
    minio_base_url = minio_api()
    print(f"[INFO] Conexión a MinIO establecida con endpoint: {minio_base_url}")

    try:
        model = output_json.get("Model", "").lower()
        print(f"[INFO] Modelo detectado: {model}")
        if "infra" in model:
            tipo = "infrarroja"
            img_data = thermic_img
        elif "multi" in model:
            tipo = "multiespectral"
            img_data = multispectral_img
        else:
            tipo = "visible"
            img_data = visible_img
        print(f"[INFO] Tipo de ráfaga determinado: {tipo}")

        tabla_captura = f"observacion_aerea.captura_rafaga_{tipo}"
        tabla_observacion = f"observacion_aerea.observation_captura_rafaga_{tipo}"
        tabla_temporal_subsample = f"observacion_aerea.observation_captura_rafaga_{tipo}_temporal_subsample"
        print(f"[INFO] Tablas seleccionadas: {tabla_captura}, {tabla_observacion}, {tabla_temporal_subsample}")

        rafaga_base = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")
        print(f"[INFO] Identificador ráfaga: {rafaga_base}, MissionID: {mission_id}")

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
        elif tipo == "infrarroja":
            calibration_id = output_json.get("calibrationId", None)
            calibracion_termica = output_json.get("calibracionTermica", None)
            insert_rafaga_sql = f"""
                INSERT INTO {tabla_captura} (
                    valid_time, payload_id, multisim_id, ground_control_station_id,
                    pc_embarcado_id, operator_name, pilot_name, sensor, platform,
                    calibrationid, calibraciontermica
                ) VALUES (
                    tsrange(now()::timestamp, (now() + interval '1 minute')::timestamp),
                    :payload_id, :multisim_id, :ground_control_station_id,
                    :pc_embarcado_id, :operator_name, :pilot_name, :sensor, :platform,
                    :calibration_id, :calibracion_termica
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
                'calibration_id': calibration_id,
                'calibracion_termica': calibracion_termica
            }
        else:  # multiespectral
            exposure0 = output_json.get("exposureTime0", None)
            exposure1 = output_json.get("exposureTime1", None)
            exposure2 = output_json.get("exposureTime2", None)
            insert_rafaga_sql = f"""
                INSERT INTO {tabla_captura} (
                    valid_time, payload_id, multisim_id, ground_control_station_id,
                    pc_embarcado_id, operator_name, pilot_name, sensor, platform,
                    exposuretime0, exposuretime1, exposuretime2
                ) VALUES (
                    tsrange(now()::timestamp, (now() + interval '1 minute')::timestamp),
                    :payload_id, :multisim_id, :ground_control_station_id,
                    :pc_embarcado_id, :operator_name, :pilot_name, :sensor, :platform,
                    :exposure0, :exposure1, :exposure2
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
                'exposure0': exposure0,
                'exposure1': exposure1,
                'exposure2': exposure2
            }

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

        image_url = None
        if img_data:
            prefix = f"rafagas/{tipo}/"
            key = f"{prefix}{uuid.uuid4()}.jpg"
            print(f"[INFO] Subiendo imagen a MinIO en {bucket}/{key}")
            upload_to_minio('minio_conn', bucket, key, img_data)
            image_url = f"{minio_base_url}/{bucket}/{key}"
            print(f"[INFO] Imagen subida. URL: {image_url}")
        else:
            print("[INFO] No se proporcionó imagen para subir a MinIO.")

        if image_url:
            insert_subsample_sql = f"""
                INSERT INTO {tabla_temporal_subsample} (
                    phenomenon_time, image_url
                ) VALUES (
                    :phenomenon_time, :image_url
                )
            """
            subsample_params = {
                "phenomenon_time": valid_time_start,
                "image_url": image_url
            }
            print(f"[INFO] Insertando subsample temporal en tabla: {tabla_temporal_subsample}")
            session.execute(text(insert_subsample_sql), subsample_params)
            print(f"[INFO] Insertado subsample temporal en {tabla_temporal_subsample}")
        else:
            print("[INFO] No se encontró imagen para subsample temporal, se omite inserción.")

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
