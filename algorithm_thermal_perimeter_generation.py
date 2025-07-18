import json
import os
import time
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from dag_utils import throw_job_error, update_job_status
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook

# Función que ejecuta el algoritmo de generación de perímetros térmicos utilizando los datos recibidos desde la interfaz y ejecuta el Docker.
def execute_thermal_perimeter_process(**context):

    conf = context.get("dag_run").conf
    if not conf:
        print("Error: No se recibió configuración desde el DAG.")
        return
    
    print("Datos recibidos del DAG:")
    print(json.dumps(conf, indent=4))

    message = conf.get("message", {})
    trace_id = conf.get("trace_id", "no-trace")
    print(f"Processing with trace_id: {trace_id}")
    
    job_id = message.get('id')
    input_data_str = message.get('input_data', '{}')
    
    try:
        input_data = json.loads(input_data_str)
    except json.JSONDecodeError:
        print("Error al decodificar 'input_data'")
        throw_job_error(job_id, "Invalid JSON in input_data")
        return

    # Extraer datos básicos de la interfaz
    mission_id = input_data.get('mission_id')
    selected_bursts = input_data.get('selected_bursts', [])
    selected_images = input_data.get('selected_images', [])
    advanced_params = input_data.get('advanced_params', {})
    
    if not mission_id:
        throw_job_error(job_id, "No se especificó ID de misión")
        return
    
    if not selected_bursts:
        throw_job_error(job_id, "No se seleccionaron ráfagas")
        return

    print("Datos extraídos correctamente:")
    print(f"Mission ID: {mission_id}")
    print(f"Selected bursts: {selected_bursts}")
    print(f"Selected images: {len(selected_images) if selected_images else 'todas'}")
    print(f"Advanced params: {advanced_params}")
    
    # Crear configuración básica para el algoritmo
    algorithm_config = create_basic_config(mission_id, selected_bursts, advanced_params, job_id)
    
    # Ejecutar Docker
    try:
        execute_docker_algorithm(algorithm_config, job_id)
        print("Algoritmo Docker ejecutado correctamente")
    except Exception as e:
        print(f"Error ejecutando algoritmo: {str(e)}")
        throw_job_error(job_id, str(e))
        raise

# Crea la configuración básica para el algoritmo R según los parámetros recibidos.
def create_basic_config(mission_id, selected_bursts, advanced_params, job_id):
    
    # Parámetros avanzados con valores por defecto
    n_clusters = advanced_params.get('numberOfClusters', 3)
    threshold_temp = advanced_params.get('tresholdTemp', 2)
    
    # Configuración según especificación del algoritmo
    config = {
        "fireId": mission_id,
        "criteria": {
            "numberOfClusters": n_clusters,
            "tresholdTemp": threshold_temp
        },
        "infraredBursts": [
            {
                "path": f"/share_data/input/incendio{mission_id}/{burst_id}",
                "id": f"PO{burst_id}",
                "timestamp": "2024-07-16T12:00:00Z",
                "sensorId": "ThermalSensor",
                "imageCount": 5
            }
            for burst_id in selected_bursts
        ]
    }
    
    return config

# Ejecuta el algoritmo de perímetros térmicos usando Docker en el servidor remoto.
def execute_docker_algorithm(config, job_id):
    
    print("Iniciando ejecución Docker...")
    
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    
    with ssh_hook.get_conn() as ssh_client:
        sftp = ssh_client.open_sftp()
        
        # Rutas en el servidor remoto
        remote_base_dir = '/home/admin3/Algoritmo_deteccion_perimetro'
        config_file = f'{remote_base_dir}/share_data/input/config_{job_id}.json'
        launch_dir = f'{remote_base_dir}/launch'
        
        # Extraer mission_id del config para el directorio de salida
        mission_id = config.get('fireId', 'default')
        
        print(f"Preparando archivos en {remote_base_dir}")
        
        # Crear directorios necesarios
        directories_to_create = [
            f'{remote_base_dir}/share_data',
            f'{remote_base_dir}/share_data/input', 
            f'{remote_base_dir}/share_data/output',
            f'{remote_base_dir}/share_data/output/incendio{mission_id}'
        ]
        
        for directory in directories_to_create:
            try:
                sftp.mkdir(directory)
                print(f"Directorio creado: {directory}")
            except Exception as e:
                print(f"Directorio ya existe: {directory}")
        
        # Guardar configuración JSON para el algoritmo R
        with sftp.file(config_file, 'w') as remote_file:
            json.dump(config, remote_file, indent=4)
        print(f"Configuración guardada: {config_file}")
        
        # Preparar imágenes para el algoritmo
        print("Preparando imágenes para procesamiento...")
        
        # Usar imágenes existentes en el servidor como plantilla
        server_template_base = f'{remote_base_dir}/input/incendio1'
        selected_bursts = [burst.get("id", "").replace("PO", "") for burst in config.get("infraredBursts", [])]

        for burst_id in selected_bursts:
            remote_burst_dir = f'{remote_base_dir}/share_data/input/incendio{mission_id}/{burst_id}'
            server_template_dir = f'{server_template_base}/{burst_id}'
            
            try:
                sftp.mkdir(f'{remote_base_dir}/share_data/input/incendio{mission_id}')
                sftp.mkdir(remote_burst_dir)
                print(f"Directorio de ráfaga creado: {remote_burst_dir}")
            except:
                print(f"Directorio ya existe: {remote_burst_dir}")
            
            # Verificar si ya hay imágenes disponibles
            try:
                files = sftp.listdir(remote_burst_dir)
                tif_files = [f for f in files if f.endswith('.tif')]
                if len(tif_files) >= 3:
                    print(f"Encontradas {len(tif_files)} imágenes en ráfaga {burst_id}")
                else:
                    # Copiar imágenes desde plantilla si están disponibles
                    try:
                        template_files = sftp.listdir(server_template_dir)
                        tif_template_files = [f for f in template_files if f.endswith('.tif')]
                        
                        if len(tif_template_files) > 0:
                            print(f"Copiando imágenes desde {server_template_dir}")
                            copy_cmd = f'cp {server_template_dir}/*.tif {remote_burst_dir}/'
                            stdin, stdout, stderr = ssh_client.exec_command(copy_cmd)
                            stdout.channel.recv_exit_status()
                            print(f"Imágenes copiadas para ráfaga {burst_id}")
                        else:
                            print(f"No hay imágenes disponibles en {server_template_dir}")
                            
                    except Exception as e:
                        print(f"Intentando copiar desde ráfaga de respaldo...")
                        fallback_cmd = f'ls {server_template_base}/1/*.tif | head -5 | xargs -I {{}} cp {{}} {remote_burst_dir}/ 2>/dev/null || true'
                        stdin, stdout, stderr = ssh_client.exec_command(fallback_cmd)
                        stdout.channel.recv_exit_status()
                        print(f"  Imágenes de respaldo copiadas para ráfaga {burst_id}")
                        
            except Exception as e:
                print(f"Error procesando ráfaga {burst_id}: {str(e)}")
        
        print("Preparación de imágenes completada")
        
        # Crear archivo .env para Docker
        env_content = f"""VOLUME_PATH={remote_base_dir}/share_data
ALG_DIR=.
CONFIGURATION_PATH=/share_data/input/config_{job_id}.json
OUTDIR=/share_data/output/incendio{mission_id}
OUTPUT=true
CONTAINER_NAME=thermal_perimeter_{job_id}
"""
        with sftp.file(f'{launch_dir}/.env', 'w') as env_file:
            env_file.write(env_content)
        print("Archivo .env creado")
        
        # Verificar que el archivo de configuración existe
        try:
            stat_info = sftp.stat(config_file)
            print(f"✓ Archivo de configuración verificado: {stat_info.st_size} bytes")
        except Exception as e:
            raise Exception(f"Error: no se pudo verificar el archivo de configuración: {e}")
        
        sftp.close()
        
        # Configurar permisos del script
        print("Configurando permisos...")
        ssh_client.exec_command(f'chmod +x {remote_base_dir}/launch/install_geospatial.sh')
        time.sleep(1)
        
        # Limpiar contenedores anteriores
        print("Limpiando contenedores anteriores...")
        cleanup_command = f'cd {launch_dir} && docker compose down --volumes'
        stdin, stdout, stderr = ssh_client.exec_command(cleanup_command)
        stdout.channel.recv_exit_status()
        time.sleep(2)
        
        # Ejecutar algoritmo
        print("Ejecutando algoritmo de perímetros térmicos...")
        
        stdin, stdout, stderr = ssh_client.exec_command(
            f'cd {launch_dir} && docker compose up --build'
        )
        
        # Esperar a que termine y obtener resultados
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error_output = stderr.read().decode()
        
        print("SALIDA DEL ALGORITMO:")
        print(output)
        
        if error_output:
            print("ERRORES:")
            print(error_output)
        
        # Verificar resultado
        if exit_status != 0:
            raise Exception(f"Docker falló con código {exit_status}: {error_output}")
        
        # Verificar códigos de estado del algoritmo R
        if "Status 1:" in output or 'No se han encontrado imágenes' in output:
            raise Exception("El algoritmo no encontró imágenes para procesar")
        elif "Status -100:" in output:
            raise Exception("Error desconocido en el algoritmo")
        elif "Status 0:" in output or "El algoritmo se ha ejecutado correctamente" in output:
            print("✓ Algoritmo completado exitosamente")
        
        # Verificar archivos de salida
        output_dir = f'{remote_base_dir}/share_data/output/incendio{mission_id}'
        expected_files = ['mosaico.tiff', 'output.json', 'perimetro.gpkg']
        
        print("Verificando archivos de salida...")
        files_found = 0
        for file_name in expected_files:
            file_path = f'{output_dir}/{file_name}'
            try:
                sftp = ssh_client.open_sftp()
                file_stat = sftp.stat(file_path)
                print(f"✓ Archivo generado: {file_name} ({file_stat.st_size} bytes)")
                files_found += 1
            except FileNotFoundError:
                print(f"✗ Archivo faltante: {file_name}")
            except Exception as e:
                print(f"? Error verificando {file_name}: {str(e)}")
        
        if files_found == 0:
            raise Exception("No se generaron archivos de salida")
        else:
            print(f"✓ Se generaron {files_found}/{len(expected_files)} archivos esperados")
        
        sftp.close()
        print("Algoritmo ejecutado correctamente")

# Función que cambia el estado del job a FINISHED cuando se completa el proceso
def change_job_status(**context):
    message = context['dag_run'].conf['message']
    job_id = message['id']
    update_job_status(job_id, 'FINISHED')

# Configuración del DAG
default_args = {
    'owner': 'sergio',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Definición del DAG principal
dag = DAG(
    'algorithm_thermal_perimeter_generation',
    default_args=default_args,
    description='DAG para generar perímetros de incendios a partir de imágenes termográficas',
    schedule_interval=None,  # Triggered by frontend/Kafka
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# Tarea principal del algoritmo
execute_algorithm_task = PythonOperator(
    task_id='execute_thermal_perimeter_algorithm',
    python_callable=execute_thermal_perimeter_process,
    provide_context=True,
    dag=dag,
)

# Cambiar estado a finalizado
change_status_task = PythonOperator(
    task_id='change_job_status_to_finished',
    python_callable=change_job_status,
    provide_context=True,
    dag=dag,
)

# Flujo del DAG
execute_algorithm_task >> change_status_task