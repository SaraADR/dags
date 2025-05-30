from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import text
from dag_utils import get_db_session
import json
from datetime import datetime, timedelta

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
        
        # Tablas temporales de subsamples
        tabla_temporal_subsample = {
            "visible": "observacion_aerea.observation_captura_rafaga_visible_temporal_subsample",
            "infrarroja": "observacion_aerea.observation_captura_rafaga_infrarroja_temporal_subsample",
            "multiespectral": "observacion_aerea.observation_captura_rafaga_multiespectral_temporal_subsample"
        }[tipo]

        rafaga_base = output_json.get("IdentificadorRafaga")
        mission_id = output_json.get("MissionID")

        print(f"Tipo detectado: {tipo}")
        print(f"Tabla captura: {tabla_captura}")
        print(f"Tabla observación: {tabla_observacion}")
        print(f"Tabla temporal subsample: {tabla_temporal_subsample}")

        # -- Construcción de valores específicos por tipo para captura_rafaga --
        if tipo == "visible":
            exposure_time = output_json.get("exposureTime", None)
            insert_rafaga_sql = f"""
                INSERT INTO {tabla_captura} (
                    valid_time, payload_id, multisim_id, ground_control_station_id,
                    pc_embarcado_id, operator_name, pilot_name, sensor, platform,
                    exposureTime
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
                    calibrationId, calibracionTermica
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
                    exposureTime0, exposureTime1, exposureTime2
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

        # Insert captura_rafaga_<tipo>
        result = session.execute(text(insert_rafaga_sql), params)
        fid_captura = result.scalar()
        print(f"Insertado en {tabla_captura} con fid: {fid_captura}")

        # --- Preparar rango temporal phenomenonTime para observation_captura_rafaga ---
        valid_time_start = datetime.utcnow()
        valid_time_end = valid_time_start + timedelta(minutes=1)
        phenomenon_time_range = f"['{valid_time_start.isoformat()}', '{valid_time_end.isoformat()}')"

        # -- Generar polígono shape (usando lon/lat y un offset pequeño) --
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
        except Exception:
            shape_wkt = "POLYGON((0 0,0 0,0 0,0 0,0 0))"
            print("Error al parsear coordenadas GPS, usando polígono 0")

        # Insertar en observation_captura_rafaga_<tipo>
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

        result_obs = session.execute(text(insert_obs_sql), obs_params)
        fid_observacion = result_obs.scalar()
        print(f"Insertada observación en {tabla_observacion} con fid: {fid_observacion}")

        # --- Insertar en tabla temporal de subsamples ---
        # Seleccionamos la imagen correcta y el campo según el tipo
        if tipo == "visible":
            imagen = output_json.get("visible_image", None)
            imagen_campo = "imagen"
        elif tipo == "infrarroja":
            imagen = output_json.get("thermic_image", None)
            imagen_campo = "imagen"
        else:
            imagen = output_json.get("multispectral_image", None)
            imagen_campo = "imagen"

        if imagen is not None:
            insert_subsample_sql = f"""
                INSERT INTO {tabla_temporal_subsample} (
                    phenomenon_time, {imagen_campo}
                ) VALUES (
                    :phenomenon_time, :imagen
                )
            """
            subsample_params = {
                "phenomenon_time": valid_time_start,
                "imagen": imagen
            }
            session.execute(text(insert_subsample_sql), subsample_params)
            print(f"Insertado subsample temporal en {tabla_temporal_subsample}")
        else:
            print("No se encontró imagen para subsample temporal, se omite inserción.")

        session.commit()

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
