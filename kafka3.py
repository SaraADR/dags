import json
import logging
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from pymongo import MongoClient


database_url = "postgresql://biodb:b10Db@vps-52d8b534.vps.ovh.net:5431/postgres"
tabla = "observacion_aerea.aeronave"

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2024, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


consumer_logger = logging.getLogger("airflow")
def consumer_function(message, prefix=None):
    message_json = json.loads(message.value().decode('utf-8'))
    consumer_logger.info(f"{prefix}  {message_json}")
    buscar_registro(message_json)
    return

def buscar_registro(message_json):
    engine = create_engine(database_url)
    with engine.connect() as connection:
        resultado = connection.execute(f"SELECT * FROM {tabla} WHERE fid = {message_json['fid']}")
        registro = resultado.first()
        if registro:
            consumer_logger.info(f"Registro encontrado para FID {message_json['fid']}: {registro}")
            return registro
        else:
            insertar_registro(message_json)
            insertar_registromongo(message_json)
            return 

def insertar_registro(message_json):
    engine = create_engine(database_url)
    with engine.connect() as connection:
        connection.execute(f"""
            INSERT INTO {tabla} (fid , matricula) VALUES ({message_json['fid']}, {message_json['matricula']})
        """)

def insertar_registromongo(message_json):
    client = MongoClient("mongodb://your_admin_username:your_admin_password@10.96.114.149:27017")
    db = client["Datalake"]
    collection = db["Datalake"]
    collection.insert_one(message_json)  


with DAG(
    "kafka_DAG",
    default_args=default_args,
    description="KafkaOperators",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["Test_DAG"],
) as dag:


    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test1"],
        apply_function=consumer_function,
        apply_function_kwargs={"prefix": "consumed:::"},
        kafka_config_id="kafka_connection",
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )