import os
import signal
import json
import time
import threading
from utility.opcua_custom_lib import servers_connection, read_nodes_from_json, removing_invalid_nodes, polling_service,  sub_and_monitored_items_service
from utility.kafka_custom_lib import connect_kafka_producer

def main():
	closing_event = threading.Event()

	with open("config.json") as f:
		try:
			data = json.load(f)
		except Exception as ex:
			print(f"ERROR, not a valid JSON!\n{ex.__class__, ex.args}\nExit...")
			os._exit(0)			

	try:
		servers = data["Servers"]
		kafka_addr = data["KafkaServer"]
	except KeyError:
		print("ERROR in JSON configuration file! Exit...")
		os._exit(0)

	# For Debug purpose to snif packets with Wireshark
	#security_policies_uri=['http://opcfoundation.org/UA/SecurityPolicy#None']	
	
	security_policies_uri=['http://opcfoundation.org/UA/SecurityPolicy#None', 'http://opcfoundation.org/UA/SecurityPolicy#Basic128Rsa15', 'http://opcfoundation.org/UA/SecurityPolicy#Basic256', 'http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256']		
	
	# json validity verification
	nodes_to_handle, subscriptions_to_handle = read_nodes_from_json(servers)
	
	# Kafka Producer
	kafka_producer = connect_kafka_producer(kafka_addr)
	
	servers_subs = [None for i in servers]
	
	# Servers connection
	clients_list, check_servers_status_thread = servers_connection(servers, security_policies_uri, nodes_to_handle, servers_subs, subscriptions_to_handle, kafka_producer, closing_event)
	
	print("\n"*3)
	print(f"{'-'*60}")
	
	def signal_handler(sig, frame):
		closing_event.set()
		print("\nWaiting for threads to finish!")
		check_servers_status_thread.join()
		for thread in polling_threads:
			thread.join()
		print("\nCLIENT STOPPED! (You pressed CTRL+C)")
		if kafka_producer is not None:
			kafka_producer.close()
		for i, c in enumerate(clients_list):	
			if c != None:				
				if servers_subs[i] != None:						
					for sub in servers_subs[i]:
						try:
							sub["sub"].delete()
							print(f"\nSubscription {sub['subId']} of server {servers[i]['address']} deleted!")
						except:
							print(f"\nUnable to delete subscription {sub['subId']} of server {servers[i]['address']}!")		
				try:
					# "ns=0;i=2259" is ServerStatus node
					c.get_node("ns=0;i=2259").get_data_value()	
					c.disconnect()					
					print(f"\nClient correctly disconnected from {servers[i]['address']}")
				except:
					print(f"\nServer {c.server_url.netloc} is probably down!")
		os._exit(0) 
	
	signal.signal(signal.SIGINT, signal_handler)
		
	sub_and_monitored_items_service(servers, clients_list, nodes_to_handle, subscriptions_to_handle, servers_subs, kafka_producer, closing_event)
	polling_threads = polling_service(servers, clients_list, nodes_to_handle, kafka_producer, closing_event)
	

	while(True):
		time.sleep(2)	
			
if __name__ == "__main__":			
	main()