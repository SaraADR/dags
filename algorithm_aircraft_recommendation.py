from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import base64
import tempfile
import os
import paramiko
from airflow.models import Variable
from airflow.hooks.base import BaseHook

def execute_algorithm_remote(**context):
    # Leer inputData desde el trigger
    input_data = context['dag_run'].conf.get('inputData', {})
    print("InputData recibido:")
    print(json.dumps(input_data, indent=2))

    # Configuración de conexión SSH
    ssh_conn = BaseHook.get_connection("ssh_avincis_2")
    hostname = ssh_conn.host
    username = ssh_conn.login  # ← usará airflow-executor

    # Obtener la clave desde la variable codificada
    ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p-2")).decode("utf-8")
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(ssh_key_decoded)
        temp_file_path = temp_file.name
    os.chmod(temp_file_path, 0o600)

    try:
        # Conectar al bastión
        bastion = paramiko.SSHClient()
        bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        bastion.connect(
            hostname=hostname,
            username=username,
            key_filename=temp_file_path
        )
        print("Conexión SSH al bastión establecida")

        # Hacer salto al servidor interno
        jump_transport = bastion.get_transport()
        jump_channel = jump_transport.open_channel(
            "direct-tcpip",
            dest_addr=("10.38.9.6", 22),
            src_addr=("127.0.0.1", 0)
        )

        # Conectar al servidor interno usando el mismo usuario
        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_client.connect(
            hostname="10.38.9.6",
            username="airflow-executor",
            sock=jump_channel,
            key_filename=temp_file_path
        )
        print("Conexión con servidor interno (10.38.9.6) establecida")

        # Ejecutar el algoritmo remoto
        # cmd = 'cd ~/algoritmo && source venv/bin/activate && python call_recomendador.py input/input_data_aeronaves.txt'
        # cmd = 'for d in /home /opt /srv /data /var /usr /root; do echo "Contenido de: $d" && ls -la $d; echo ""; done'
        cmd = 'ls -l /algoritms/algoritmo-asignacion-aeronaves-objetivo-5'


        print(f"Ejecutando comando remoto:\n{cmd}")
        stdin, stdout, stderr = target_client.exec_command(cmd)

        print("STDOUT:")
        print(stdout.read().decode())

        print("STDERR:")
        print(stderr.read().decode())

        # Cierre de conexiones
        target_client.close()
        bastion.close()

    finally:
        os.remove(temp_file_path)

default_args = {
    'owner': 'sara',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'algorithm_aircraft_recommendation',
    default_args=default_args,
    description='Ejecuta algoritmo de recomendación remoto en Avincis',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

process_element_task = PythonOperator(
    task_id='execute_algorithm_via_jump_host',
    python_callable=execute_algorithm_remote,
    provide_context=True,
    dag=dag,
)


# def process_element(**context):
#     # print("Algoritmo de asignación de aeronaves")
#     # message = context['dag_run'].conf
#     # input_data_str = message['message']['input_data']
#     # input_data = json.loads(input_data_str)
#     # task_type = message['message']['job']
#     # from_user = message['message']['from_user']
#     # print(input_data)

#     # Ruta temporal para almacenar la clave privada en el contenedor
  
#     try:
#         ssh_key_decoded = base64.b64decode(Variable.get("ssh_avincis_p")).decode("utf-8")

#         if ssh_key_decoded.startswith("-----BEGIN ") and "PRIVATE KEY-----" in ssh_key_decoded:
#             print("La clave tiene un formato válido.")
#         else:
#             print("La clave no parece ser válida.")

#         with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
#             temp_file.write(ssh_key_decoded)
#             temp_file_path = temp_file.name
#         os.chmod(temp_file_path, 0o600)

#         jump_host_hook = SSHHook(
#             ssh_conn_id='ssh_avincis',
#             key_file=temp_file_path
#         )

#         try:
#             # Establecer conexión
#             with jump_host_hook.get_conn() as ssh_client:
#                 # Ejecutar un comando remoto (por ejemplo, listar archivos en el directorio home)
#                 stdin, stdout, stderr = ssh_client.exec_command('ls -l')
                
#                 # Leer y mostrar la salida del comando
#                 print("Salida del comando:")
#                 print(stdout.read().decode())

#                 # Leer errores si los hay
#                 print("Errores (si existen):")
#                 print(stderr.read().decode())
#         except Exception as e:
#             print(f"Error al intentar conectarse o ejecutar un comando: {e}")


#         with jump_host_hook.get_conn() as jump_host_client:
#             print("Conexión SSH con máquina intermedia exitosa")

#             transport = jump_host_client.get_transport()
#             dest_addr = ('10.38.9.6', 22)
#             local_addr = ('127.0.0.1', 0)

#             jump_channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)

#             second_client = paramiko.SSHClient()
#             second_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

#             second_client.connect(
#                 '10.38.9.6',
#                 username='airflow-executor',
#                 sock=jump_channel,
#                 key_filename=temp_file_path
#             )
#             print("Conexión SSH con servidor privado exitosa")

#             stdin, stdout, stderr = second_client.exec_command('ls')
#             print("Archivos en el servidor destino:")
#             print(stdout.read().decode())
#             print("Errores (si hay):")
#             print(stderr.read().decode())

#             second_client.close()

#     finally:
#         os.remove(temp_file_path)

# default_args = {
#     'owner': 'sadr',
#     'depends_on_past': False,
#     'start_date': datetime.datetime(2025, 1, 3),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': datetime.timedelta(minutes=1),
# }

# dag = DAG(
#     'algorithm_aircraft_recommendation',
#     default_args=default_args,
#     description='Algoritmo de asignación de aeronaves',
#     schedule_interval=None, 
#     catchup=False,
#     max_active_runs=1,
#     concurrency=1
# )

# process_element_task = PythonOperator(
#     task_id='process_message',
#     python_callable=process_element,
#     provide_context=True,
#     dag=dag,
# )

# process_element_task


