import os
import json
from time import sleep
import signal
import threading
import uuid

from kafka import KafkaConsumer

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


def read_from_kafka(topic_name, kafka_addr, db, closing_event):
		try:
			kafka_consumer = KafkaConsumer(topic_name, auto_offset_reset='latest',
								 bootstrap_servers=[kafka_addr], 
								 #enable_auto_commit=True, group_id='my-group', 
								 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
		except:
			kafka_consumer = None
			
		else:		
			for msg in kafka_consumer:
				
				if closing_event.is_set(): break
			
				message = msg.value
				# message is a dictionary
				print(f"\nMessage: {message}")
				
				# Post message on Firebase database
				doc = str(message["server"]+"-"+message["node"]+"-"+message["serverTimestamp"]+"-"+ str(uuid.uuid4()))
				#print(f"\nDoc: {doc}")
				doc_ref = db.collection("OpcUaNodes").document(doc)
				doc_ref.set({
					'value': str(message["value"]),
					'node': str(message["node"]),
					'server': str(message["server"]),
					'sourcetimestamp': str(message["sourceTimestamp"]),
					'servertimestamp': str(message["serverTimestamp"]),
					'status': str(message["status"])
				})
  
				#print(result)			
			
		if kafka_consumer is not None:
			kafka_consumer.close()		
	
def main():
	
	def signal_handler(sig, frame):
		print("\nCONSUMER STOPPED! (You pressed CTRL+C)")
		closing_event.set() 
		#thread_opc.join()
		os._exit(0)

	signal.signal(signal.SIGINT, signal_handler)
	
		
	with open("config_kafkaConsumer.json") as f:
		try:
			data = json.load(f)
		except Exception as ex:
			print(f"ERROR, not a valid JSON!\n{ex.__class__, ex.args}\nExit...")
			os._exit(0)		
	
	
	# Use a service account
	cred = credentials.Certificate('opcua-client-kafka-gateway-firebase-adminsdk-hn72v-4c7137a285.json')
	firebase_admin.initialize_app(cred)

	db = firestore.client()
		
			
	try:
		kafka_addr = data["KafkaServer"]
		topic_name = data["topic"]
		#firebase_collection = data["firebaseCollection"]
		
	except KeyError:
		print("ERROR in JSON configuration file! Exit...")
		os._exit(0)	
					 
	
	print("\nCONSUMER STARTED...Press CTRL+C to STOP.")
	
	closing_event = threading.Event()
	
	thread_opc = threading.Thread(target=read_from_kafka, args=(topic_name, kafka_addr, db, closing_event))
	thread_opc.start()
	
	while(not closing_event.is_set()):
		sleep(1)
	
if __name__ == '__main__':
	main()
	