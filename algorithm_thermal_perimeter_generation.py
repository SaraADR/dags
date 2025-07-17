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
                "path": f"/share_data/input/burst_{burst_id}",
                "id": str(burst_id),
                "timestamp": "2024-07-16T12:00:00", 
                "sensorId": "thermal_sensor",
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
        remote_base_dir = '/home/admin3/thermal-perimeter-algorithm'
        config_file = f'{remote_base_dir}/share_data/input/config_{job_id}.json'
        launch_dir = f'{remote_base_dir}/launch'
        
        print(f"Preparando archivos en {remote_base_dir}")
        
        # Crear directorios necesarios
        try:
            sftp.mkdir(f'{remote_base_dir}/share_data')
            sftp.mkdir(f'{remote_base_dir}/share_data/input')
            sftp.mkdir(f'{remote_base_dir}/share_data/output')
        except:
            pass  # Directorios ya existen
        
        # Guardar configuración JSON para el algoritmo R
        with sftp.file(config_file, 'w') as remote_file:
            json.dump(config, remote_file, indent=4)
        print(f"✅ Configuración guardada: {config_file}")
        
        # Crear archivo .env dinámico
        env_content = f"""VOLUME_PATH=..
ALG_DIR=.
CONFIGURATION_PATH=share_data/input/config_{job_id}.json
OUTDIR=share_data/output
OUTPUT=true
CONTAINER_NAME=thermal_perimeter_{job_id}
"""
        with sftp.file(f'{launch_dir}/.env', 'w') as env_file:
            env_file.write(env_content)
        print("Archivo .env creado")
        
        sftp.close()
        
        # Ejecutar Docker Compose
        print("Limpiando contenedores anteriores...")
        ssh_client.exec_command(f'cd {launch_dir} && docker compose down --volumes')
        time.sleep(2)
        
        print("Ejecutando algoritmo de perímetros térmicos...")
        stdin, stdout, stderr = ssh_client.exec_command(
            f'cd {launch_dir} && docker compose up --build'
        )
        
        # Leer salida del algoritmo
        output = stdout.read().decode()
        error_output = stderr.read().decode()
        exit_status = stdout.channel.recv_exit_status()
        
        print("SALIDA DEL ALGORITMO")
        print(output)
        
        if error_output:
            print("ERRORES")
            print(error_output)
        
        # Verificar resultado
        if exit_status != 0:
            raise Exception(f"Docker falló con código {exit_status}: {error_output}")
        
        # Verificar códigos de estado específicos del algoritmo R
        if "Status 1:" in output:
            raise Exception("No se encontraron imágenes en las rutas especificadas")
        elif "Status -100:" in output:
            raise Exception("Error desconocido en el algoritmo")
        elif "Status 0:" in output:
            print("Algoritmo completado exitosamente (Status 0)")
        
        print("Docker ejecutado correctamente")

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

# Definición del DAG
dag = DAG(
    'algorithm_thermal_perimeter_generation',
    default_args=default_args,
    description='DAG para generar perímetros de incendios a partir de imágenes termográficas seleccionadas en la interfaz',
    schedule_interval=None,  # Triggered by Kafka
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


    

