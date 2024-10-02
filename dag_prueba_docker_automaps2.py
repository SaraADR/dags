import datetime
import io
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.email import EmailOperator
from sqlalchemy import create_engine, Table, MetaData
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker
import json


def process_element(**context):
    message = context['dag_run'].conf
    input_data_str = message['message']['input_data']
    input_data = json.loads(input_data_str)

    # Obtener los nuevos valores de location y perimeter
    location = input_data['input']['location']
    perimeter = input_data['input'].get('perimeter', None)

    print(f"Location: {location}")
    print(f"Perimeter: {perimeter}")
    
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:
            sftp = ssh_client.open_sftp()

            print(f"Sftp abierto")

            remote_directory = '/home/admin3/Autopymaps/share_data/input'
            remote_file_name = 'config.json'
            remote_file_path = os.path.join(remote_directory, remote_file_name)

            sftp.chdir(remote_directory)
            print(f"Cambiando al directorio: {remote_directory}")

            # Leer el contenido actual del archivo config.json
            with sftp.file(remote_file_path, 'r') as remote_file:
                config_data = json.load(remote_file)  # Leer y parsear el contenido del JSON
                print("Contenido del archivo original:")
                print(config_data)


            if location is not None:
                config_data['location'] = location
            # if perimeter is not None:
            #     config_data['perimeter'] = perimeter

            print("Datos actualizados:")
            print(config_data)

            # Guardar los cambios de nuevo en el archivo
            with sftp.file(remote_file_path, 'w') as remote_file:
                json.dump(config_data, remote_file, indent=4)  # Escribir el JSON actualizado
                print(f"Archivo {remote_file_name} actualizado en {remote_directory}")


            sftp.close()
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")



def find_the_folder():
    ssh_hook = SSHHook(ssh_conn_id='my_ssh_conn')

    try:
        # Conectarse al servidor SSH
        with ssh_hook.get_conn() as ssh_client:

            # Cambiar al directorio de lanzamiento y ejecutar run.sh
            print(f"Cambiando al directorio de lanzamiento y ejecutando run.sh")
            stdin, stdout, stderr = ssh_client.exec_command('cd /home/admin3/Autopymaps/launch && ./run.sh')
            
            output = stdout.read().decode()
            error_output = stderr.read().decode()

            print("Salida de run.sh:")
            print(output)
            
            if error_output:
                print("Errores al ejecutar run.sh:")
                print(error_output)


            sftp = ssh_client.open_sftp()
            output_directory = '/home/admin3/Autopymaps/share_data/output'
            local_output_directory = '/tmp'
              
            # Crear el directorio local si no existe
            os.makedirs(local_output_directory, exist_ok=True)

            sftp.chdir(output_directory)
            print(f"Cambiando al directorio de salida: {output_directory}")

            for filename in sftp.listdir():
                remote_file_path = os.path.join(output_directory, filename)
                local_file_path = os.path.join(local_output_directory, filename)

                # Descargar cada archivo
                sftp.get(remote_file_path, local_file_path)
                print(f"Archivo {filename} descargado a {local_file_path}")

            sftp.close()

            print_directory_contents(local_output_directory)


    except Exception as e:
        print(f"Error en el proceso: {str(e)}")





def print_directory_contents(directory):
    print(f"Contenido del directorio: {directory}")
    for root, dirs, files in os.walk(directory):
        level = root.replace(directory, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{subindent}{f}")
    print("------------------------------------------")





def change_state_job(**context):
    message = context['dag_run'].conf
    job_id = message['message']['id']
    print(f"jobid {job_id}" )

    try:
   
        # Conexión a la base de datos usando las credenciales almacenadas en Airflow
        db_conn = BaseHook.get_connection('biobd')
        connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        
       

        # Update job status to 'FINISHED'
        metadata = MetaData(bind=engine)
        jobs = Table('jobs', metadata, schema='public', autoload_with=engine)
        update_stmt = jobs.update().where(jobs.c.id == job_id).values(status='FINISHED')
        session.execute(update_stmt)
        session.commit()
        print(f"Job ID {job_id} status updated to FINISHED")

    except Exception as e:
        session.rollback()
        print(f"Error durante el guardado del estado del job: {str(e)}")

 



default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

dag = DAG(
    'dag_prueba_docker2',
    default_args=default_args,
    description='Algoritmo dag_prueba_docker',
    schedule_interval=None,
    catchup=False
)

# Manda correo
process_element_task = PythonOperator(
    task_id='process_message',
    python_callable=process_element,
    provide_context=True,
    dag=dag,
)



#Cambia estado de job
find_the_folder_task = PythonOperator(
    task_id='ejecutar_run',
    python_callable=find_the_folder,
    provide_context=True,
    dag=dag,
)

#Cambia estado de job
change_state_task = PythonOperator(
    task_id='change_state_job',
    python_callable=change_state_job,
    provide_context=True,
    dag=dag,
)

process_element_task >> find_the_folder_task  >> change_state_task

