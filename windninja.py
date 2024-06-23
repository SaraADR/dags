from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 17),
    'retries': 1
}


def windninjafunction():
    sistema_operativo = "windows"
    comando = ["windninja.exe", os.path.join(directorio_output, "vientos", "windninjafile.txt")]

    try:
        # Ejecutar el comando con un límite de tiempo de 5 minutos
        proceso = subprocess.run(comando, timeout=300, check=True)
        return proceso.returncode  # Código de retorno del proceso
    except subprocess.TimeoutExpired:
        return {"error": -6, "mensaje_error": "El tiempo de ejecución del comando excedió el límite de 5 minutos."}
    except subprocess.CalledProcessError as e:
        return {"error": e.returncode, "mensaje_error": str(e)}
    
    
with DAG(
    'windninja', 
    default_args=default_args, 
    schedule_interval=None
    ) as dag: 

    # Define la tarea que ejecutará el script de Spark
    spark_task = BashOperator(
        task_id='windninja',
        apply_function=windninjafunction,
        dag=dag
    )

    # Puedes agregar más tareas aquí si necesitas hacer otras cosas después del trabajo de Spark

    spark_task





