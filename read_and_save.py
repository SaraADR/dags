from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


def read_text_and_convert_to_json():
    # Leer el archivo de texto
    with open("/archivos/dato.txt", "r") as file:
        text_data = file.read()
    
    # Convertir el texto a JSON
    json_data = json.loads(text_data)

    # Retorna los datos JSON
    return json_data


def save_json_to_postgres(json_data):
    # Conexión y operación de guardado en PostgreSQL
    # Reemplaza 'nombre_de_tabla' con el nombre de tu tabla en PostgreSQL
    insert_sql = """
    INSERT INTO observacion_aerea.aeronave (columna_json)
    VALUES (%s);
    """

    # Ejecutar la operación de guardado en PostgreSQL
    # Reemplaza 'connection_id' con el ID de tu conexión a PostgreSQL en Airflow
    PostgresOperator(
        task_id="save_to_postgres",
        postgres_conn_id="postgres_bio",
        sql=insert_sql,
        parameters=(json.dumps(json_data),),
        dag=dag
    )



default_args = {
    'owner': 'sadr',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='read_txt_and_save_to_postgres',
    default_args=default_args,
    description='Read text file, convert to JSON and save to PostgreSQL',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily'
) as dag:

    read_and_convert_task = PythonOperator(
        task_id='read_and_convert',
        python_callable=read_text_and_convert_to_json
    )

    save_to_postgres_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_json_to_postgres,
        op_args=[read_and_convert_task.output],
    )

    read_and_convert_task >> save_to_postgres_task
