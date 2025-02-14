import datetime
import json
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

def execute_docker_process(**context):
    """Sube input_automatic.json, ejecuta Docker Compose y gestiona el contenedor"""

    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            # Subir input_automatic.json directamente en la ruta correcta
            with sftp.file("/home/admin3/grandes-incendios-forestales/input_automatic.json", "w") as json_file:
                json.dump(
                    {
                        "incendios": [
                            {"id": 1, "lat": 42.56103, "long": -8.618725},
                            {"id": 2, "lat": 43.01234, "long": -7.54321}
                        ]
                    },
                    json_file,
                    ensure_ascii=False,
                    indent=4
                )
            print("Archivo de entrada subido correctamente.")

            # Cambiar al directorio correcto y ejecutar limpieza de volúmenes
            print("Cambiando al directorio de lanzamiento y ejecutando limpieza de volúmenes")
            ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose down --volumes"
            )

            # Construir la imagen de Docker antes de ejecutar el contenedor
            print("Construyendo la imagen de Docker...")
            stdin, stdout, stderr = ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose build"
            )
            print(stdout.read().decode())
            print(stderr.read().decode())

            # Ejecutar Docker Compose en modo demonio para asegurarse de que el contenedor se cree
            print("Ejecutando Docker Compose...")
            stdin, stdout, stderr = ssh_client.exec_command(
                "cd /home/admin3/grandes-incendios-forestales && docker-compose up -d"
            )
            print(stdout.read().decode())
            print(stderr.read().decode())

            # Esperar unos segundos para permitir que el contenedor arranque
            print("Esperando 10 segundos para que el contenedor inicie correctamente...")
            time.sleep(10)

            # Verificar si el contenedor se ha creado
            print("Verificando si el contenedor gifs_service se ha creado correctamente...")
            stdin, stdout, stderr = ssh_client.exec_command(
                "docker ps -a --filter name=gifs_service --format '{{{{.ID}}}}'"
            )
            container_id = stdout.read().decode().strip()

            if container_id:
                print(f"Contenedor gifs_service encontrado: {container_id}.")
            else:
                raise Exception("Error: No se pudo crear el contenedor gifs_service.")

            # Esperar a que se genere output.json en la nueva ubicación
            print("Esperando resultado...")
            while True:
                try:
                    sftp.stat("/home/admin3/grandes-incendios-forestales/share_data/expected/output.json")
                    print("Resultado generado en output.json")
                    break
                except FileNotFoundError:
                    time.sleep(5)

            # Descargar output.json
            sftp.get("/home/admin3/grandes-incendios-forestales/share_data/expected/output.json", "/tmp/output.json")
            print("Archivo de salida descargado correctamente.")

            # Eliminar el contenedor después de la ejecución
            print("Eliminando contenedor gifs_service...")
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
