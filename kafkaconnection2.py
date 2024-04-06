from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'sadr',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
}

with DAG(
    'kafka_airflow_integration', 
    default_args=default_args,
    schedule_interval='@daily',
    ) as dag:

    t2 = ConsumeFromTopicOperator(
        kafka_config_id="kafka_connection",
        task_id="Consume_topic_test1_kafka",
        topics=["test1"],
        #apply_function="example_dag_hello_kafka.consumer_function",
        #apply_function_kwargs={"prefix": "consumed:::"},
        commit_cadence="end_of_batch",
        max_messages=1,
        max_batch_size=5,
        do_xcom_push=True
    )

 # Mostrar el mensaje consumido en la consola

    print_message_1 = BashOperator(
        task_id='print_message_1',
        bash_command="echo 'Esto esta sucediendo de verdad! Pasa por aqui'"
    )

    print_message_2 = BashOperator(
        task_id='print_message_2',
        bash_command="echo 'Message consumed: {{ ti.xcom_pull(task_ids=\"Consume_topic_test1_kafka\") }}'"
    )

    # Establecer la secuencia de tareas
    t2 >> print_message_1 >> print_message_2