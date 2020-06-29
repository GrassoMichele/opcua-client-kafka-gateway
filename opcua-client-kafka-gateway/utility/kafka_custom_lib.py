from kafka import KafkaProducer
from json import dumps

def connect_kafka_producer(kafka_address = "localhost:9092"):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=kafka_address,
								api_version=(0, 10, 1),
								security_protocol="SASL_SSL",
								sasl_mechanism='SCRAM-SHA-256', 
								sasl_plain_username='5qkeygj1', 
								sasl_plain_password='QovYFGlHVR5VG3SrKvOEqxrDeH9dk21o',
								key_serializer=lambda x: x.encode('utf-8'))
								
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(kafka_producer_info, key, value):
	try:
		producer_instance = kafka_producer_info["instance"]
		topic = kafka_producer_info["topic"]
		fernet_obj = kafka_producer_info["fernet_obj"]

		value_enc = fernet_obj.encrypt(dumps(value).encode('utf-8'))

		producer_instance.send(topic, key=key, value=value_enc)
		producer_instance.flush()
	#print('Message published successfully.')
	except Exception as ex:
		print('Exception in publishing message')
		print(str(ex))