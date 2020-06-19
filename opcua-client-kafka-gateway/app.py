import os
import signal
import json
import time
from utility.opcua_custom_lib import servers_connection, read_nodes_from_json, removing_invalid_nodes, sub_and_monitored_items_creation, check_servers_status, polling_and_monitoring_service 

def main():
	with open("config.json") as f:
		try:
			data = json.load(f)
		except Exception as ex:
			print(f"ERROR, not a valid JSON!\n{ex.__class__, ex.args}\nExit...")
			os._exit(0)			

	try:
		servers = data["Servers"]
		kafka_addr = data["KafkaServer"]
		pollingRate = data["PollingRate"]
		publishingInterval = data["PublishingInterval"]
	except KeyError:
		print("ERROR in JSON configuration file! Exit...")
		os._exit(0)

	security_policies_uri=['http://opcfoundation.org/UA/SecurityPolicy#None']
	#security_policies_uri=['http://opcfoundation.org/UA/SecurityPolicy#None', 'http://opcfoundation.org/UA/SecurityPolicy#Basic128Rsa15', 'http://opcfoundation.org/UA/SecurityPolicy#Basic256', 'http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256']		
	
	# verify json validity
	nodes_to_handle = read_nodes_from_json(servers)
	
	#Servers connection
	clients_list = servers_connection(servers, security_policies_uri)
	
	print("\n"*3 , f"{'-'*60}")
	
	#Variables Management
	removing_invalid_nodes(clients_list, servers, nodes_to_handle)
	
	subs, queues = [None for i in clients_list], [None for i in clients_list]
	
	def signal_handler(sig, frame):
		print("\nCLIENT STOPPED! (You pressed CTRL+C)")
		for i, c in enumerate(clients_list):	
			if c != None:
				try:
					if subs[i] != None: 
						subs[i][0].delete()
						print(f"\nSubscription {subs[i][0].subscription_id} deleted!")
				except:
					print(f"\nUnable to delete subscription {subs[i][0].subscription_id}!")		
				try:
					# "ns=0;i=2259" is ServerStatus node
					c.get_node("ns=0;i=2259").get_data_value()	
					c.disconnect()					
					print(f"\nClient correctly disconnected from {servers[i]['address']}")
				except:
					print(f"\nServer {c.server_url.netloc} is probably down!")
		os._exit(0) 
	
	signal.signal(signal.SIGINT, signal_handler)
	
	sub_and_monitored_items_creation(clients_list, servers, nodes_to_handle, subs, queues, publishingInterval)
			
	time.sleep(0.1)

	counter = 1 
	while(True):
		#Check on new working servers and servers that are working no more.
		check_servers_status(servers, clients_list, nodes_to_handle, subs, queues, security_policies_uri, publishingInterval)
		print(f"\n{'*'*15} ITERATION n. {counter} {'*'*15}")
		polling_and_monitoring_service(servers, clients_list, nodes_to_handle, queues)
		
		counter += 1
		time.sleep(pollingRate)	
			
if __name__ == "__main__":			
	main()