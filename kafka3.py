import json
import logging
from tkinter import Variable
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from airflow.providers.mongo.hooks.mongo import MongoHook

database_url = "postgresql://biodb:b10Db@vps-52d8b534.vps.ovh.net:5431/postgres"
tabla = "observacion_aerea.aeronave"

default_args = {
    "owner": "Sadr",
    "depend_on_past": False,
    "start_date": datetime(2024, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


consumer_logger = logging.getLogger("airflow")
def consumer_function(message, prefix=None):
    if message != None :
        message_json = json.loads(message.value().decode('utf-8'))
        consumer_logger.info(f"{prefix} {message_json}")
        Variable.set("message_json", message_json)
        return message_json
    else: 
        return None

def buscar_registro(message_json  , **kwargs):
    engine = create_engine(database_url)
    message_json = Variable.get("message_json", default_var=None)
    if message_json != None :
        with engine.connect() as connection:
            resultado = connection.execute(f"SELECT * FROM {tabla} WHERE fid = {message_json['fid']}")
            registro = resultado.first()
        if registro:
            consumer_logger.info(f"Registro encontrado para FID {message_json['fid']}: {registro}")
            return registro
        else:
            insertar_registro(message_json)
            return 
    else:
         consumer_logger.info(f"No hay datos para guardar en la base de datos de bio")
    

def insertar_registro(message_json):
    engine = create_engine(database_url)
    with engine.connect() as connection:
        connection.execute(f"""
            INSERT INTO {tabla} (fid , matricula) VALUES ({message_json['fid']},  {message_json['matricula']} )
        """)


def on_failure_callback():
    print(f"Task mongo failed.")

def uploadtomongo(message_json , **kwargs):
    print(f"data mongo {message_json}")
    message_json = Variable.get("message_json", default_var=None)
    try:
        hook = MongoHook(mongo_conn_id='mongoid')
        client = hook.get_conn()
        db = client['Datalake']
        collection = db['Datalake']
        print(f"data {message_json}")
        # Insertar el JSON en la colección
        collection.insert_one(message_json)
        print(f"Connected to MongoDB - {client.server_info()}")
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")



with DAG(
    "kafka_DAG",
    default_args=default_args,
    description="KafkaOperators",
    schedule_interval=timedelta(minutes=2),
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

    
    save_to_mongodb_task = PythonOperator(
        task_id='save_to_mongodb',
        python_callable=uploadtomongo,
        op_kwargs={'message_json': t2.output},
        provide_context=True,
        dag=dag,
    )


    buscar_registro_task = PythonOperator(
        task_id='change_data',
        python_callable=buscar_registro,
        op_kwargs={'message_json': t2.output}, 
        dag=dag,
    )
t2.set_downstream(save_to_mongodb_task)
t2.set_downstream(buscar_registro_task)
