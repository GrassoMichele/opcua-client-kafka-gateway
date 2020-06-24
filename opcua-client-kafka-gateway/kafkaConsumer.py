import os
import json
from time import sleep
import signal
import threading
from kafka import KafkaConsumer
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError

def read_from_kafka(topic_name, kafka_addr, blob_service_client, closing_event):
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
				print(f"\nTopic: {topic_name}\n {message}")		
				# Create a blob client
				try:
					blob_name = message["server"] + "\\" + message["node"]+ "\\" + message["sourceTimestamp"]
					blob_client = blob_service_client.get_blob_client(container=topic_name, blob=blob_name)
					# Upload the created file
					blob_client.upload_blob(json.dumps(message))
					#print("\nBlob created!")
				except ResourceExistsError:
					print("\nBlob already exists!") 
				except:
					print("\nError with blob creation!")
				
				sleep(0.1)
			
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

	try:
		azure_blob_connection_string = data["AzureBlobStorageConnectionString"]
		kafka_addr = data["KafkaServer"]
	except KeyError:
		print("ERROR in JSON configuration file! Exit...")
		os._exit(0)
		
	blob_service_client = BlobServiceClient.from_connection_string(azure_blob_connection_string)
	# Create the container	
	topic_name = "opcua-nodes"
	try:
		blob_service_client.create_container(topic_name)
		print(f"Container {topic_name} created!")
	except ResourceExistsError:
		print(f"\nContainer {topic_name} already exists!") 						 
	
	print("\nCONSUMER STARTED...Press CTRL+C to STOP.")
	
	closing_event = threading.Event()
	
	thread_opc = threading.Thread(target=read_from_kafka, args=(topic_name, kafka_addr, blob_service_client, closing_event))
	thread_opc.start()

	while(not closing_event.is_set()):
		sleep(0.1)
	
	
if __name__ == '__main__':
	main()
	