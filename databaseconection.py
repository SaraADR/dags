from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime

# Reemplaza estas variables con tu información de conexión a la base de datos
database_url = "postgresql://biodb:b10Db@vps-52d8b534.vps.ovh.net:5431/postgres"

tabla = "observacion_aerea.aeronave"

def buscar_registro(id_registro):
    engine = create_engine(database_url)
    with engine.connect() as connection:
        resultado = connection.execute(f"SELECT * FROM {tabla} WHERE fid = {id_registro}")
        return resultado.first()

def insertar_registro(id_registro):
    engine = create_engine(database_url)
    with engine.connect() as connection:
        connection.execute(f"""
            INSERT INTO {tabla} (fid , matricula) VALUES ({id_registro}, 'test')
        """)

with DAG(
    dag_id="databaseconnection",
    schedule_interval="@once",
    start_date=datetime(2021, 7, 29, 2),
) as dag:

    t1 = PythonOperator(
        task_id="buscar_registro",
        python_callable=buscar_registro,
        op_args=[2], # Reemplaza 123 con el ID del registro que deseas verificar
    )

    t2 = PythonOperator(
        task_id="insertar_registro",
        python_callable=insertar_registro,
        op_args=[2], # Reemplaza 123 con el ID del registro que deseas insertar
    )

    t1 >> t2