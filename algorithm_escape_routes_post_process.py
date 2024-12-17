from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from sqlalchemy import create_engine, Table, MetaData
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook


def process_escape_routes_data(**context):
    # Obtener los datos del contexto del DAG
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)

    # Procesar "inicio" y "destino" para permitir diferentes estructuras
    inicio = input_data.get('inicio', None)
    destino = input_data.get('destino', None)

    # Ajustar "inicio" y "destino" según los datos recibidos
    if isinstance(inicio, str):
        try:
            inicio = json.loads(inicio)
        except json.JSONDecodeError:
            inicio = None

    if isinstance(destino, list):
        try:
            destino = [json.loads(d) if isinstance(d, str) else d for d in destino]
        except json.JSONDecodeError:
            destino = None
    elif isinstance(destino, dict):
        pass

    # Extraer los argumentos necesarios
    params = {
        "dir_incendio": input_data.get('dir_incendio', None),
        "dir_mdt": input_data.get('dir_mdt', None),
        "dir_hojasmtn50": input_data.get('dir_hojasmtn50', None),
        "dir_combustible": input_data.get('dir_combustible', None),
        "api_idee": input_data.get('api_idee', True),
        "dir_vias": input_data.get('dir_vias', None),
        "dir_cursos_agua": input_data.get('dir_cursos_agua', None),
        "dir_aguas_estancadas": input_data.get('dir_aguas_estancadas', None),
        "inicio": inicio,
        "destino": destino,
        "direccion_avance": input_data.get('direccion_avance', None),
        "distancia": input_data.get('distancia', None),
        "dist_seguridad": input_data.get('dist_seguridad', None),
        "dir_obstaculos": input_data.get('dir_obstaculos', None),
        "dir_carr_csv": input_data.get('dir_carr_csv', None),
        "dir_output": '/share_data/output/' + 'rutas_escape_' + str(message['message']['id']),
        "sugerir": input_data.get('sugerir', False),
        "zonas_abiertas": input_data.get('zonas_abiertas', None),
        "v_viento": input_data.get('v_viento', None),
        "f_buffer": input_data.get('f_buffer', 100),
        "c_prop": input_data.get('c_prop', "Extremas"),
        "lim_pendiente": input_data.get('lim_pendiente', None),
        "dist_estudio": input_data.get('dist_estudio', 5000),
    }

    # Crear el JSON dinámicamente
    json_data = create_json(params)

    # Mostrar el JSON por pantalla
    print("JSON generado:")
    print(json.dumps(json_data, indent=4))



    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')
    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()
            print(f"Sftp abierto")

            id_ruta = str(message['message']['id'])
            carpeta_destino = f"/home/admin3/algoritmo-rutas-de-escape-algoritmo-2-master/input/input_{id_ruta}_rutas_escape"
            
            print(f"Creando carpeta y guardando el json en su interior: {carpeta_destino}")
            ssh_client.exec_command(f"mkdir -p {carpeta_destino}")
            json_file_path = f"{carpeta_destino}/input_data_{id_ruta}.json"

            ssh_client.exec_command(f"touch -p {json_file_path}")

            # with sftp.file(json_file_path, 'w') as json_file:
            #     json.dumps(json_data, json_file, indent=4)
            # print(f"Archivo JSON guardado en: {json_file_path}")


            command = f'cd /home/admin3/algoritmo-rutas-de-escape-algoritmo-2-master/launch &&  docker-compose -f compose.yaml up --build'
            stdin, stdout, stderr = ssh_client.exec_command(command)
            output = stdout.read().decode()
            error_output = stderr.read().decode()
            exit_status = stdout.channel.recv_exit_status() 
            if exit_status != 0:
                print("Errores al ejecutar run.sh:")
                print(error_output)

            print("Salida de docker:")
            print(output)

    except Exception as e:
        print(f"Error en el proceso: {str(e)}")

    finally:
        # Cerrar SFTP si está abierto
        if 'sftp' in locals():
            sftp.close()
            print("SFTP cerrado.")

    # Actualizar el estado del job a 'FINISHED' si todo se completa correctamente
    try:
        job_id = message['message']['id']
        update_job_status(job_id, "FINISHED")
    except Exception as e:
        print(f"Error al actualizar el estado del trabajo: {e}")
        raise

def create_json(params):
    # Generar un JSON basado en los parámetros proporcionados
    input_data = {
        "dir_incendio": params.get("dir_incendio", None),
        "dir_mdt": params.get("dir_mdt", None),
        "dir_hojasmtn50": params.get("dir_hojasmtn50", None),
        "dir_combustible": params.get("dir_combustible", None),
        "api_idee": params.get("api_idee", True),
        "dir_vias": params.get("dir_vias", None),
        "dir_cursos_agua": params.get("dir_cursos_agua", None),
        "dir_aguas_estancadas": params.get("dir_aguas_estancadas", None),
        "inicio": params.get("inicio", None),
        "destino": params.get("destino", None),
        "direccion_avance": params.get("direccion_avance", None),
        "distancia": params.get("distancia", None),
        "dist_seguridad": params.get("dist_seguridad", None),
        "dir_obstaculos": params.get("dir_obstaculos", None),
        "dir_carr_csv": params.get("dir_carr_csv", None),
        "dir_output": params.get("dir_output", None),
        "sugerir": params.get("sugerir", False),
        "zonas_abiertas": params.get("zonas_abiertas", None),
        "v_viento": params.get("v_viento", None),
        "f_buffer": params.get("f_buffer", 100),
        "c_prop": params.get("c_prop", "Extremas"),
        "lim_pendiente": params.get("lim_pendiente", None),
        "dist_estudio": params.get("dist_estudio", 5000),
    }
    return input_data

def update_job_status(job_id, status):
    try:
        # Conexión a la base de datos usando las credenciales de Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        metadata = MetaData(bind=engine)

        # Tabla de trabajos
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)

        # Actualizar el estado del trabajo
        with engine.connect() as connection:
            update_stmt = jobs.update().where(jobs.c.id == job_id).values(status=status)
            connection.execute(update_stmt)
            print(f"Job ID {job_id} status updated to {status}")
    except Exception as e:
        print(f"Error al actualizar el estado del trabajo: {e}")
        raise

# Configuración del DAG
default_args = {
    'owner': 'oscar',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_escape_routes_post_process',
    default_args=default_args,
    description='DAG para generar JSON, mostrarlo por pantalla y actualizar estado del job',
    schedule_interval=None,
    catchup=False,
    concurrency=1
)

# Tarea para generar y mostrar el JSON
process_escape_routes_task = PythonOperator(
    task_id='process_escape_routes',
    provide_context=True,
    python_callable=process_escape_routes_data,
    dag=dag,
)

# Definir el flujo de tareas
process_escape_routes_task
