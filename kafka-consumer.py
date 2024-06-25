from confluent_kafka import Consumer
from airflow.hooks.base import BaseHook

def get_kafka_consumer(connection_id):
    connection = BaseHook.get_connection(connection_id)
    broker = connection.host
    group_id = connection.extra_dejson.get('group_id')
    
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    return consumer

def consume_kafka_message(consumer, topic):
    consumer.subscribe([topic])
    
    msg = consumer.poll(1.0)
    if msg is None:
        return None
    if msg.error():
        raise Exception(msg.error())
    
    return msg.value()