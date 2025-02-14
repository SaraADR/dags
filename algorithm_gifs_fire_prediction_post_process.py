import datetime
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

def execute_docker_process(**context):
    """Recoge los datos de entrada, valida los datos, sube input_automatic.json, ejecuta Docker Compose y gestiona el contenedor"""

    ssh_hook = SSHHook(ssh_conn_id="my_ssh_conn")

    try:
        # Recoger datos de entrada desde Airflow
        dag_run_conf = context.get("dag_run").conf
        input_data = dag_run_conf.get("incendios", [])

        # Validar que los datos sean correctos
        if not input_data or not isinstance(input_data, list):
            raise ValueError("Los datos de entrada no son válidos o están vacíos.")

        for item in input_data:
            if "id" not in item or "lat" not in item or "long" not in item:
                raise ValueError(f"Datos incorrectos en el incendio: {item}")

        print("Datos de entrada validados correctamente:", input_data)

        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            # Subir input_automatic.json con los datos correctos
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

            # Intentar descargar output.json si existe
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
