import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, Table, MetaData
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.orm import sessionmaker
import pytz

def consumer_function(message, prefix, **kwargs):
    if message is not None:
        msg_value = message.value().decode('utf-8')
        print(f"message2: {msg_value}")
        if msg_value:
            try:
                print(f"message4: {msg_value}")
                Variable.set("mensaje_save", msg_value)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Empty message received")
            Variable.set("mensaje_save", None)        
            return None  
    else:
        Variable.set("mensaje_save", None)        

def trigger_email_handler(**kwargs):
    try:
        value_pulled = Variable.get("mensaje_save")
        print(f"pulled {value_pulled}")
    except KeyError:
        print("Variable mensaje_save does not exist")
        raise AirflowSkipException("Variable mensaje_save does not exist")
    

    if value_pulled is not None and value_pulled != 'null':
        try:
            msg_json = json.loads(value_pulled)
            print(msg_json)
            
            try:
            #Insertamos la mision
            db_conn = BaseHook.get_connection('biobd')
            connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
            engine = create_engine(connection_string)
            Session = sessionmaker(bind=engine)
            session = Session()

            #Pasamos las fechas de timestamp a datetime
            if(msg_json.get('start') is not None):
                start_date  = convert_millis_to_datetime(msg_json.get('start'))

            if(msg_json.get('creation_timestamp') is not None):
                creation_date  = convert_millis_to_datetime(msg_json.get('creation_timestamp'))    

            mss_mission_insert = {
                'name': msg_json.get('name', 'noname'),
                'start_date': msg_json.get(start_date, datetime.now()),
                'geometry': msg_json.get('position'),
                'type_id': 3,
                'customer_id': 'infoca',
                'creationtimestamp': creation_date,
                'status_id': 1
            }
            

            metadata = MetaData(bind=engine)
            mission = Table('mss_mission', metadata, schema='missions', autoload_with=engine)

            # Inserción de la relación
            insert_stmt = mission.insert().values(mss_mission_insert)
            session.execute(insert_stmt)
            session.commit()
            session.close()

            mission_id = result.inserted_primary_key[0]
            print(f"Misión creada con ID: {mission_id}")
            except Exception as e:
                session.rollback()
                print(f"Error durante el guardado de la misión: {str(e)}")

            try:
                if (mission_id is not None):
                    #Insertamos la mision_fire
                    db_conn = BaseHook.get_connection('biobd')
                    connection_string = f"postgresql://{db_conn.login}:{db_conn.password}@{db_conn.host}:{db_conn.port}/postgres"
                    engine = create_engine(connection_string)
                    Session = sessionmaker(bind=engine)
                    session = Session()

                    mss_mission_fire_insert = {
                        'mission_id': mission_id,
                        'fire_id': msg_json.get('id'),,
                        'ignition_timestamp': msg_json.get(start_date, datetime.now())
                    }
                

                    metadata = MetaData(bind=engine)
                    mission_fire = Table('mss_mission_fire', metadata, schema='missions', autoload_with=engine)

                    # Inserción de la relación
                    insert_stmt = mission.insert().values(mss_mission_fire_insert)
                    session.execute(insert_stmt)
                    session.commit()
                    session.close()
            except Exception as e:
                session.rollback()
                print(f"Error durante el guardado de la misión: {str(e)}")
            Variable.delete("mensaje_save")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    else:
        print("No message pulled from XCom")
        Variable.delete("mensaje_save")


def convert_millis_to_datetime(millis):
    try:
        # Convertir millis a entero
        millis = int(millis)
    except ValueError:
        raise ValueError(f"Invalid millisecond value: {millis}")

    seconds = millis / 1000.0
    dt_utc = datetime.fromtimestamp(seconds, pytz.utc)
    return dt_utc


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_consumer_einforex_dag',
    default_args=default_args,
    description='DAG que consume mensajes de la base de datos de einforex',
    schedule_interval='*/1 * * * *',
    catchup=False
)

consume_from_topic = ConsumeFromTopicOperator(
    kafka_config_id="kafka_connection",
    task_id="consume_from_topic",
    topics=["einforex"],
    apply_function=consumer_function,
    apply_function_kwargs={"prefix": "consumed:::"},
    commit_cadence="end_of_batch",
    max_messages=1,
    max_batch_size=1,
    dag=dag,
)

trigger_email_handler_task = PythonOperator(
    task_id='trigger_email_handler',
    python_callable=trigger_email_handler,
    provide_context=True,
    dag=dag,
)


consume_from_topic >> trigger_email_handler_task