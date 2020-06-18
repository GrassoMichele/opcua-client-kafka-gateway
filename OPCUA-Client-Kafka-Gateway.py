import sys, os
import signal
import json
from textwrap import dedent
import time
import logging
import queue
from opcua import Client, ua
from opcua.ua.uaprotocol_auto import MessageSecurityMode, DataChangeFilter, DataChangeTrigger, DeadbandType
from opcua.ua.uatypes import NodeId
from opcua.common.node import Node
# The following three lines of imports are for BadSessionClosed handling.
from opcua.client import ua_client
from opcua.ua.ua_binary import struct_from_binary
from opcua.ua.uaerrors import BadTimeout, BadNoSubscription, BadSessionClosed

def best_endpoint_selection(client, index, printable=False):
	endpoints = client.connect_and_get_server_endpoints()

	if printable: print(f"\n\n{'-'*10} SERVER: {servers[index]['address']} {'-'*10}")
	
	best_sec_lvl = -1
	for i, endpoint in enumerate(endpoints):
		if printable: print(dedent(f"""
			{'-'*10} Endpoint {i+1} {'-'*10}
			Security level: {endpoint.SecurityLevel}
			EndpointUrl: {endpoint.EndpointUrl}
			SecurityMode : {MessageSecurityMode(endpoint.SecurityMode).name}
			SecurityPolicyUri : {endpoint.SecurityPolicyUri}
			TransportProfileUri : {endpoint.TransportProfileUri}
			"""))
		if endpoint.SecurityLevel > best_sec_lvl and endpoint.SecurityPolicyUri in security_policies_uri: 
			best_endpoint = endpoint
			best_sec_lvl = best_endpoint.SecurityLevel	
	
	if printable: print(dedent(f"""
		{'-'*10} BEST ENDPOINT SELECTED {'-'*10}
		Security level: {best_endpoint.SecurityLevel}
		EndpointUrl: {best_endpoint.EndpointUrl}
		SecurityMode : {MessageSecurityMode(best_endpoint.SecurityMode).name}
		SecurityPolicyUri : {best_endpoint.SecurityPolicyUri}
		TransportProfileUri : {best_endpoint.TransportProfileUri}
		"""))
	return best_endpoint
	
def user_authentication(client):			
	while(True):
		auth_selection = input(dedent(
		"""
		Authentication Mode:
		1. Anonymous
		2. Username
		3. Certificate (not working)
		: """)) 
		
		if auth_selection == str(1):
			break
		elif auth_selection == str(2):
			username = input("\nUsername: ")
			password = input("\nPassword: ")
			client.set_user(username)
			client.set_password(password)
			break
		elif auth_selection == str(3):
			client.load_client_certificate("client_certificate.pem")
			client.load_private_key("client_key.pem")
			break
		else:
			print("\nSelection not allowed!")
	return client
	
def client_connection(index, auth=False, session_name=False, printable=False):
	client = Client(servers[index]["address"])
	try:
		best_endpoint = best_endpoint_selection(client, index, printable)
		client = Client(best_endpoint.EndpointUrl)		
		client.application_uri = "urn:freeopcua:client"

		policy = best_endpoint.SecurityPolicyUri.split('#')[1]		
		if policy != "None": 
			security_string = str(policy) + ',' + str(MessageSecurityMode(best_endpoint.SecurityMode).name) + ',client_certificate.pem' + ',client_key.pem' 
			client.set_security_string(security_string)		

		if auth: client = user_authentication(client)	
		client.description = input("\nSession name: ") if session_name != False else "OPC UA - Kafka_Gateway_Client"
		
		client.connect()
		clients_list[index] = client
		working_servers[index] = servers[index]
		return client		
	except:
		return
	#DOMANDA: Server Application Instance Certificate validation?	

def servers_connection(servers):
    # Servers connection
	for i,s in enumerate(servers):							
		client = client_connection(i, printable=True) 
		if client == None: 
			print(f"\nERROR: Server {s['address']} is not available!")
		else:
			print(f"\nConnected to Server {s['address']}!")
	return clients_list, working_servers

def read_nodes_from_json(servers):
    # Nodes setting function
	nodes_to_handle = []
	for s in servers:
		nodes_to_read_s, nodes_to_monitor_s = [], []
		for i,v in enumerate(s["Variables"]):
			# var = "ns=x;i=y" or "ns=x;s=z"
			try:
				var = "ns="+str(v["nodeId"]["ns"])+";"
				var += f"s={v['nodeId']['s']}" if "s" in v['nodeId'].keys() else f"i={v['nodeId']['i']}"		
				if v["Publishing"]["type"] == "polling":
					nodes_to_read_s.append(var)
				elif v["Publishing"]["type"] == "monitoredItem":				
					if "filter" in v["Publishing"].keys():
						filter = v["Publishing"]["filter"]
					else:
						filter = "None"
					nodes_to_monitor_s.append({"node":var, "filter":filter})
				else:
					print(f"\nError in server {s['address']} with the node type of {var}!")
					raise KeyError
			except KeyError:
					print(f"\nError in server {s['address']} with the node {i+1}!")
					# remove wrong nodes
					s["Variables"].remove(v)
								
		nodes_to_handle.append(tuple([nodes_to_read_s, nodes_to_monitor_s]))
	return nodes_to_handle

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
	
class SubHandler(object):
	def datachange_notification(self, node, val, data):
		try:
			i = [c.uaclient if c is not None else None for c in clients_list].index(node.server)
			notification = {"node":node, "value":data.monitored_item.Value.Value, "status":data.monitored_item.Value.StatusCode, "sourceTimestamp":data.monitored_item.Value.SourceTimestamp, "serverTimestamp":data.monitored_item.Value.ServerTimestamp}
			queues[i].put(notification)
		except ValueError:
			pass		

def _create_monitored_items(sub, dirty_nodes, attr, queuesize=0):
	mirs = []
	for dirty_node in dirty_nodes:
		if dirty_node["filter"] != "None":
			try:
				mfilter = DataChangeFilter()
				#DataChangeTrigger(0) = Status, DataChangeTrigger(1) = StatusValue, DataChangeTrigger(2) = StatusValueTimestamp
				mfilter.Trigger = DataChangeTrigger[dirty_node["filter"]["Trigger"]]
				# DeadbandType(0) = None, DeadbandType(1) = Absolute, DeadbandType(2) = Percent
				mfilter.DeadbandType = DeadbandType[dirty_node["filter"]["DeadbandType"]]
				mfilter.DeadbandValue = dirty_node["filter"]["DeadbandValue"]
			except:
				mfilter = None
				print(f"\n\nWARNING: Can't apply filter on node {dirty_node['node']}")
		else:
			mfilter = None
		node_var = Node(sub.server, NodeId().from_string(dirty_node["node"]))
		mir = sub._make_monitored_item_request(node_var, attr, mfilter, queuesize)
		mirs.append(mir)
	mids = sub.create_monitored_items(mirs)
	return mids

# removing invalid (not in address space) nodes from running servers 
def removing_invalid_nodes(working_servers):
	# following line should permit to read (Polling service) or monitor (Monitored Items service) previously missing nodes in servers address allowing to add them subsequently (anyway they have to appear in json file).
	nodes_to_handle = read_nodes_from_json(servers)
	for i, s in enumerate(working_servers):
		if s != None:
			# monitored items nodes 
			for n in nodes_to_handle[i][1]:
				try:
					clients_list[i].get_node(n["node"]).get_data_value()	
				except:
					print(f"\n\nWARNING: Node {n['node']} of server {s['address']} for Monitored Item service removed because it's not in address space.\n\n")
					nodes_to_handle[i][1].remove(n)
			
			# polling nodes 
			for n in nodes_to_handle[i][0]:
				try:
					clients_list[i].get_node(n).get_data_value()	
				except:
					print(f"\n\nWARNING: Node {n['node']} of server {s['address']} for Polling service removed because it's not in address space.\n\n")
					nodes_to_handle[i][0].remove(n)
					
def sub_and_monitored_items_creation(subs, queues):
	# we are assuming a subscription per client			
	for i, s in enumerate(working_servers):
		if s != None and subs[i]==None and len(nodes_to_handle[i][1]) > 0:
			# create subscription and monitored items 
			sub = clients_list[i].create_subscription(publishingInterval, handler)
			handle = _create_monitored_items(sub, nodes_to_handle[i][1], ua.AttributeIds.Value)	
			subs[i] = tuple((sub, handle))
			queues[i] = queue.Queue()		
	
def check_servers_status(servers):
	for i, s in enumerate(servers):
		try:
			# "ns=0;i=2259" is ServerStatus node
			clients_list[i].get_node("ns=0;i=2259").get_data_value()	
		except:
			# check if previously working servers are still up
			if working_servers[i] != None:
				print(f"\nERROR: SERVER {s['address']} IS DOWN!")
				working_servers[i], clients_list[i], subs[i] = None, None, None
				
			# check if previously down servers are now up
			if working_servers[i] == None:
				# best_endpoint_selection and client_connection  
				client = client_connection(i, printable=False)
				if client != None: print(f"\nCONNECTED to Server {s['address']}!")				
				# sub and monitored_items creation 
				sub_and_monitored_items_creation(subs, queues)				
				
	# this way we remove wrong nodes from NEW servers and it should remove a node if exists no more (has been deleted).
	removing_invalid_nodes(working_servers)
	

def polling_and_monitoring_service (working_servers, nodes_to_handle, clients_list, queues):
	# READ SERVICE e MONITORED ITEMS notifications function
	global counter
	counter+=1
	print(f"\n{'*'*15} ITERATION n. {counter} {'*'*15}")
	for i, s in enumerate(working_servers): 	
		print(f"\n\n{'-'*10} SERVER: {servers[i]['address']} {'-'*10}")
		if s != None:		
			print("\nREAD service: ")
			if len(nodes_to_handle[i][0]) > 0:				
				for n in nodes_to_handle[i][0]:
					try:
						data_v = clients_list[i].get_node(n).get_data_value()	
						print(dedent(f"""
						Node: {n}
						Value: {data_v.Value.Value} 
						StatusCode: {data_v.StatusCode.name} 
						SourceTimestamp: {data_v.SourceTimestamp} 
						ServerTimestamp: {data_v.ServerTimestamp}
						"""))
					except:
						# credo che si possa verificare solo se succede qualcosa al server o viene rimosso il nodo
						print(f"\nDetected a problem with node {n} from this server.")							
			else:
				print("NO nodes to read for this server!")
			print("\nMONITORED ITEMS notifications collected: ")
			if queues[i] == None: 
				print("NO Monitored Items for this server!")
				continue
			if queues[i].empty(): print("NO notifications for this server!")
			while(True):
				try:			 
					mon_item_notif = queues[i].get_nowait()
					print(dedent(f"""
					Node: {mon_item_notif['node']}
					Value: {mon_item_notif['value']} 
					StatusCode: {mon_item_notif['status']} 
					SourceTimestamp: {mon_item_notif['sourceTimestamp']} 
					ServerTimestamp: {mon_item_notif['serverTimestamp']}
					"""))
				except queue.Empty:
					break
			"""except:
			print("\nServer disconnected, connection retrying!")
			clients_list.pop(i)
			working_servers.pop(i)
			client = client_connection(url) 
			if client != None: 
				
				clients_list.append(client)
				working_servers.append(s)"""
		else:
			print("Server is DOWN!")

# This function is a patching of _call_publish_callback function from UaClient module to handle unexpected server closing.
def our_call_publish_callback(self, future):
	self.logger.info("call_publish_callback")
	data = future.result()
	# check if answer looks ok
	try:
		self._uasocket.check_answer(data, "while waiting for publish response")
	except BadTimeout:  # Spec Part 4, 7.28
		self.publish()
		return
	except BadNoSubscription:  # Spec Part 5, 13.8.1
		self.logger.info("BadNoSubscription received, ignoring because it's probably valid.")
		return
	except BadSessionClosed:
		self.logger.info("\nBadSessionClosed received!")
		return
	# parse publish response
	try:
		response = struct_from_binary(ua.PublishResponse, data)
		self.logger.debug(response)
	except Exception:
		self.logger.exception("Error parsing notificatipn from server")
		self.publish([])  # send publish request ot server so he does stop sending notifications
		return
	# look for callback
	try:
		callback = self._publishcallbacks[response.Parameters.SubscriptionId]
	except KeyError:
		self.logger.warning("Received data for unknown subscription: %s ", response.Parameters.SubscriptionId)
		return
	# do callback
	try:
		callback(response.Parameters)
	except Exception:  # we call client code, catch everything!
		self.logger.exception("Exception while calling user callback: %s")

			

if __name__ == "__main__":	
	ua_client.UaClient._call_publish_callback = our_call_publish_callback	
	logging.getLogger('opcua').setLevel(logging.CRITICAL)
	
	sys.path.insert(0, "..")
	signal.signal(signal.SIGINT, signal_handler)

	with open("config.json") as f:
		data = json.load(f)

	servers = data["Servers"]
	kafka_addr = data["KafkaServer"]
	pollingRate = data["PollingRate"]
	publishingInterval = data["PublishingInterval"]

	# DA TOGLIERE: con la security policy None e Anonymous funziona, con il resto altri server oltre al Sample NO (forse per via del certificato?)!
	security_policies_uri = ['http://opcfoundation.org/UA/SecurityPolicy#None']
	#security_policies_uri=['http://opcfoundation.org/UA/SecurityPolicy#None', 'http://opcfoundation.org/UA/SecurityPolicy#Basic128Rsa15', 'http://opcfoundation.org/UA/SecurityPolicy#Basic256', 'http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256']

	#Server connection
	clients_list, working_servers = [None for s in servers],[None for s in servers]
	servers_connection(servers)

	print("\n"*3 , f"{'-'*60}")
	
	#Variables Management
	nodes_to_handle = read_nodes_from_json(servers)

	# MONITORED ITEM function
	handler = SubHandler()				#una per ogni monitoredItem?
	
	removing_invalid_nodes(working_servers)
	
	subs = [None for i in clients_list]
	queues = [None for i in clients_list]
	
	sub_and_monitored_items_creation(subs, queues)
			
	time.sleep(0.1)

	counter = 0 
	while(True):
		#Check on new working servers and servers that are working no more.
		check_servers_status(servers)
		polling_and_monitoring_service(working_servers, nodes_to_handle, clients_list, queues)
		time.sleep(pollingRate)