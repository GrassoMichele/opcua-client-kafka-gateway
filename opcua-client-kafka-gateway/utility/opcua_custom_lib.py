import sys
from textwrap import dedent
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

from .kafka_custom_lib import publish_message


def read_nodes_from_json(servers):
    # Nodes setting function
	nodes_to_handle = []
	wrong_json = False
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
					wrong_json = True
					# remove wrong nodes
					s["Variables"].remove(v)
								
		nodes_to_handle.append(tuple([nodes_to_read_s, nodes_to_monitor_s]))
	if wrong_json:
		if(input("ERRORS in JSON configuration file.\nPress y to continue (ignoring wrong nodes), any other key to exit: ")!="y"):
			sys.exit(0)		
	return nodes_to_handle

def best_endpoint_selection(client, server, security_policies_uri, printable=False):
	endpoints = client.connect_and_get_server_endpoints()

	if printable: print(f"\n\n{'-'*10} SERVER: {server['address']} {'-'*10}")
	
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

def client_auth(client, server):
	auth_types = ("anonymous", "username/password", "certificate")	
	try:
		# dictionaries list
		user_auth = server["auth"]
		if user_auth["type"] in auth_types:
			if user_auth["type"] == "username/password":
				client.set_user(user_auth["username"])
				client.set_password(user_auth["password"])
			elif user_auth["type"] == "certificate":
				client.load_client_certificate(user_auth["cert_path"])
				client.load_private_key(user_auth["private_key_path"])				
		else:
			print(f"\nUser authentication type for server {server['address']} is not valid! Trying Anonymous logon.")
	except:
		print(f"\nError in JSON configuration file. User authentication for server {server['address']} failed! Trying Anonymous logon.")
	return client 
	
def client_connection(index, server, security_policies_uri, printable=False):
	client = Client(server["address"])	
	try:
		best_endpoint = best_endpoint_selection(client, server, security_policies_uri, printable)
		
		client = Client(best_endpoint.EndpointUrl)	
		client.application_uri = "urn:freeopcua:client"
		client.description = "OPCUA-Client-Kafka-Gateway"
		
		client = client_auth(client, server)
		
		policy = best_endpoint.SecurityPolicyUri.split('#')[1]		
		if policy != "None": 
			security_string = str(policy) + ',' + str(MessageSecurityMode(best_endpoint.SecurityMode).name) + ',client_certificate.pem' + ',client_key.pem' 
			client.set_security_string(security_string)	
		
		client.connect()		
		return client		
	except Exception as ex:
		print(f"\nEXCEPTION in client connection: {ex.__class__, ex.args}")
		return
	#DOMANDA: Server Application Instance Certificate validation?	

def servers_connection(servers, security_policies_uri):
	clients_list = [None for s in servers]
	for i,s in enumerate(servers):	
		s["working"] = False
		client = client_connection(i, s, security_policies_uri, printable=True) 
		if client == None: 
			print(f"\nERROR: Cannot connect to server: {s['address']}!")
			# workaround for username/password and certificate authentication not accepted from server.
			print(f"\nTrying connection with Anonymous logon...")
			s["auth"]["type"] = "anonymous"
			client = client_connection(i, s, security_policies_uri)
			if client == None:
				print(f"\nTrying connection with None security policy...")
				client = client_connection(i, s, ['http://opcfoundation.org/UA/SecurityPolicy#None'])
				if client != None:
					clients_list[i] = client
					s["working"] = True
					print(f"\nConnected to Server {s['address']}!")
			else:
				clients_list[i] = client
				s["working"] = True
				print(f"\nConnected to Server {s['address']}!")
		else:
			clients_list[i] = client
			s["working"] = True
			print(f"\nConnected to Server {s['address']}!")
	return clients_list

# removing invalid (not in address space) nodes from running servers 
def removing_invalid_nodes(clients_list, servers, nodes_to_handle):
	# recall this function should permit to read (Polling service) or monitor (Monitored Items service) previously missing nodes in servers address allowing to add them subsequently (anyway they have to appear in json file).
	for i, s in enumerate(servers):
		if s["working"] != False:
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

class SubHandler(object):
	def __init__(self, clients_list, queues):
		self.clients_list = clients_list
		self.queues = queues
		
	def datachange_notification(self, node, val, data):
		try:
			i = [c.uaclient if c is not None else None for c in self.clients_list].index(node.server)
			notification = {"server":self.clients_list[i].server_url.netloc, "node":node.nodeid.to_string(), "value":val, "status":data.monitored_item.Value.StatusCode.name, "sourceTimestamp":str(data.monitored_item.Value.SourceTimestamp), "serverTimestamp":str(data.monitored_item.Value.ServerTimestamp)}
			self.queues[i].put(notification)
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

					
def sub_and_monitored_items_creation(clients_list, servers, nodes_to_handle, subs, queues, publishingInterval):	
	handler = SubHandler(clients_list, queues)		
	# we are assuming a subscription per client			
	for i, s in enumerate(servers):
		if s["working"] != False and subs[i]==None and len(nodes_to_handle[i][1]) > 0:
			# create subscription and monitored items 
			sub = clients_list[i].create_subscription(publishingInterval, handler)
			handle = _create_monitored_items(sub, nodes_to_handle[i][1], ua.AttributeIds.Value)	
			subs[i] = tuple((sub, handle))
			queues[i] = queue.Queue()
	
def check_servers_status(servers, clients_list, nodes_to_handle, subs, queues, security_policies_uri, publishingInterval):
	for i, s in enumerate(servers):
		try:
			# "ns=0;i=2259" is ServerStatus node
			clients_list[i].get_node("ns=0;i=2259").get_data_value()	
		except:
			# check if previously working servers are still up
			if s["working"] != False:
				print(f"\nERROR: SERVER {s['address']} IS DOWN!")
				s["working"], clients_list[i], subs[i] = False, None, None
				
			# check if previously down servers are now up
			if s["working"] == False:
				# best_endpoint_selection and client_connection  
				client = client_connection(i, s, security_policies_uri, printable=False)
				if client != None: 
					clients_list[i] = client
					s["working"] = True
					print(f"\nCONNECTED to Server {s['address']}!")				
				# sub and monitored_items creation 
				sub_and_monitored_items_creation(clients_list, servers, nodes_to_handle, subs, queues, publishingInterval)
					
	# this way we remove wrong nodes from NEW servers and it should remove a node if exists no more (has been deleted).
	removing_invalid_nodes(clients_list, servers, nodes_to_handle)
	

def polling_and_monitoring_service (servers, clients_list, nodes_to_handle, queues, kafka_producer):
	# READ SERVICE e MONITORED ITEMS notifications function
	for i, s in enumerate(servers): 	
		print(f"\n\n{'-'*10} SERVER: {servers[i]['address']} {'-'*10}")
		if s["working"]!= False:		
			print("\nREAD service: ")
			if len(nodes_to_handle[i][0]) > 0:				
				for n in nodes_to_handle[i][0]:
					try:
						data_v = clients_list[i].get_node(n).get_data_value()
						data_to_send = {"server":clients_list[i].server_url.netloc, "node":n, "value":data_v.Value.Value, "status":data_v.StatusCode.name, "sourceTimestamp":str(data_v.SourceTimestamp), "serverTimestamp":str(data_v.ServerTimestamp)}
						print(dedent(f"""
						Server: {data_to_send['server']}
						Node: {data_to_send['node']}
						Value: {data_to_send['value']} 
						StatusCode: {data_to_send['status']} 
						SourceTimestamp: {data_to_send['sourceTimestamp']} 
						ServerTimestamp: {data_to_send['serverTimestamp']}
						"""))
						publish_message(kafka_producer, "opcua-polling", 'node', data_to_send)
					except:
						# this could be true only if something happens to server or a node has been removed.
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
					Server: {mon_item_notif['server']}
					Node: {mon_item_notif['node']}
					Value: {mon_item_notif['value']} 
					StatusCode: {mon_item_notif['status']} 
					SourceTimestamp: {mon_item_notif['sourceTimestamp']} 
					ServerTimestamp: {mon_item_notif['serverTimestamp']}
					"""))
					publish_message(kafka_producer, "opcua-monitoreditems", 'node', mon_item_notif)
				except queue.Empty:
					break
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

ua_client.UaClient._call_publish_callback = our_call_publish_callback
logging.getLogger('opcua').setLevel(logging.CRITICAL)	
sys.path.insert(0, "..")