from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from PIL import Image
from io import BytesIO
import filetype
import base64

default_args = {
    'owner': 'ssadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 6),
}

def process_message(messages, **kwargs):
    for message in messages:
        content = message.value
        kind = filetype.guess(content)

        if kind is not None and kind.mime.startswith('image/'):
            image = Image.open(BytesIO(content))
            image_metadata = image.info
            print("Image metadata:", image_metadata)
        else:
            print("Text message:", content.decode('utf-8'))


with DAG(
    'KAINT', 
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    ) as dag:

    t2 = ConsumeFromTopicOperator(
        kafka_config_id="kafka_connection",
        task_id="Consume_topic_test1_kafka",
        topics=["test1"],
        #apply_function="example_dag_hello_kafka.consumer_function",
        #apply_function_kwargs={"prefix": "consumed:::"},
        commit_cadence="end_of_batch",
        max_messages=5,
        max_batch_size=5,
        do_xcom_push=True
    )

 # Mostrar el mensaje consumido en la consola

    print_message_1 = BashOperator(
        task_id='print_message_1',
        bash_command="echo 'Leyendo el mensaje que ha llegado a kafka .. .. .. .. .. .. .. .. ..'"
    )

    process_message_task = PythonOperator(
        task_id='process_message_task',
        python_callable=process_message,
        op_args=[[{"value": base64.b64decode(m)} for m in "{{ ti.xcom_pull(task_ids='Consume_topic_test1_kafka') }}"]],
        provide_context=True,
    )

    

    # Establecer la secuencia de tareas
    t2 >> print_message_1 >> process_message_task