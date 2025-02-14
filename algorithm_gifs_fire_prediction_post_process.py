import datetime
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

def execute_docker_process(**context):
    """Sube input_automatic.json con datos de prueba, ejecuta Docker Compose y gestiona el contenedor"""

    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            # Usar un JSON de prueba (más adelante reemplazar con datos reales)
            input_data = [
                {"id": 1, "lat": 42.56103, "long": -8.618725},
                {"id": 2, "lat": 43.01234, "long": -7.54321}
            ]

            print("Usando JSON de prueba para la ejecución:", input_data)

            # Subir input_automatic.json con los datos de prueba
            input_path = "/home/admin3/grandes-incendios-forestales/input_automatic.json"
            with sftp.file(input_path, "w") as json_file:
                json.dump({"incendios": input_data}, json_file, ensure_ascii=False, indent=4)

            print(f"Archivo de entrada subido correctamente: {input_path}")

            # Cambiar al directorio correcto y ejecutar limpieza de volúmenes
            print("Ejecutando limpieza de volúmenes...")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose down --volumes"
            )

            # Construir la imagen de Docker antes de ejecutar el contenedor
            print("Construyendo la imagen de Docker...")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose build"
            )

            # Ejecutar Docker Compose en modo demonio
            print("Ejecutando Docker Compose...")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose up -d"
            )

            # Intentar descargar output.json si existe, sin esperar activamente
            output_path = "/home/admin3/grandes-incendios-forestales/share_data_host/expected/output.json"
            local_output_path = "/tmp/output.json"

            try:
                sftp.get(output_path, local_output_path)
                print("Archivo de salida descargado correctamente.")
            except FileNotFoundError:
                print("output.json no encontrado. Continuando con la ejecución sin descargar.")

            # Eliminar el contenedor después de la ejecución
            print("Eliminando contenedor...")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose down"
            )
            print("Contenedor eliminado correctamente.")

            sftp.close()

    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")
        raise

# Configuración del DAG en Airflow
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

dag = DAG(
    'algorithm_gifs_fire_prediction_post_process',
    default_args=default_args,
    description='DAG to execute GIFS Fire Prediction Algorithm with Docker Compose',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# Definición de tarea en el DAG
execute_docker_task = PythonOperator(
    task_id='execute_docker_process',
    python_callable=execute_docker_process,
    provide_context=True,
    dag=dag,
)

# Definir la secuencia de tareas
execute_docker_task
