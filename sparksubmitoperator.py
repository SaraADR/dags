from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
import base64

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
}

# Guardar los mensajes de Kafka en un archivo temporal
def save_messages_to_file(messages, **kwargs):
    temp_file_path = "/temp/messages.txt"
    with open(temp_file_path, 'w') as f:
        for message in messages:
            f.write(base64.b64encode(message['value']).decode('utf-8') + '\n')
    return temp_file_path

with DAG(
    'kafka_airflow_spark_integration', 
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    t2 = ConsumeFromTopicOperator(
        kafka_config_id="kafka_connection",
        task_id="Consume_topic_test1_kafka",
        topics=["test1"],
        commit_cadence="end_of_batch",
        max_messages=5,
        max_batch_size=5,
        do_xcom_push=True
    )

    save_messages_task = PythonOperator(
        task_id='save_messages_task',
        python_callable=save_messages_to_file,
        op_args=[[{"value": m} for m in "{{ ti.xcom_pull(task_ids='Consume_topic_test1_kafka') }}"]],
        provide_context=True,
    )

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_process_kafka_messages',
        application="process_kafka_messages.py",
        name='kafka_message_processor',
        conf={'spark.master': 'spark://10.96.115.197:7077'},
        application_args=["{{ ti.xcom_pull(task_ids='save_messages_task') }}"]
    )

    t2 >> save_messages_task >> spark_submit_task
