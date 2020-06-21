from kafka import KafkaProducer
from json import dumps

def connect_kafka_producer(kafka_address = "localhost:9092"):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[kafka_address], key_serializer=lambda x: x.encode('utf-8'), value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, key, value):
    try:
        producer_instance.send(topic_name, key=key, value=value)
        producer_instance.flush()
        #print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))