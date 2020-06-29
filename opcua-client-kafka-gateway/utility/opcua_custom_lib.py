import sys
from textwrap import dedent
import logging
import queue
import threading
from time import sleep
from datetime import datetime

from opcua import Client, ua
from opcua.ua.uaprotocol_auto import MessageSecurityMode, DataChangeFilter, DataChangeTrigger, DeadbandType, ReadParameters, CreateSubscriptionParameters, TimestampsToReturn, ReadValueId
from opcua.common.subscription import SubscriptionItemData
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
	subscriptions_to_handle = []
	wrong_json = False
	for i,s in enumerate(servers):
		server_subscriptions = []	
		try:		
			for subscription in s["subscriptions"]:
				server_subscriptions.append(subscription)
			if len(server_subscriptions) > 0:
				subscriptions_to_handle.append(server_subscriptions)
			else:
				subscriptions_to_handle = None
		except:
			print("\nNo possible subscription for server {s['address']}!")
			
		nodes_to_read_s, nodes_to_monitor_s = [], []
		for i,v in enumerate(s["Variables"]):
			try:
				var = "ns="+str(v["nodeId"]["ns"])+";"
				var += f"s={v['nodeId']['s']}" if "s" in v['nodeId'].keys() else f"i={v['nodeId']['i']}"		
				if v["Publishing"]["type"] == "polling":
					node_to_read = {}
					node_to_read["node"] = var
					node_to_read["properties"]={}
					try:					
						node_to_read["properties"]["maxAge"]=v["Publishing"]["maxAge"]
						node_to_read["properties"]["timestampsToReturn"]=v["Publishing"]["timestampsToReturn"]
						node_to_read["properties"]["pollingRate"]=v["Publishing"]["pollingRate"]
					except:
						pass
					nodes_to_read_s.append(node_to_read)
							
				elif v["Publishing"]["type"] == "monitoredItem":
					
					if v["Publishing"]["subId"] not in [sub["subId"] for sub in server_subscriptions]:
					 	print(f"Invalid subscription selected for node {var} of server{s['address']}")
					 	wrong_json = True
					 	continue
					subId = v["Publishing"]["subId"]
					try:
						samplingInterval = v["Publishing"]["samplingInterval"]
						monitoringMode = v["Publishing"]["monitoringMode"]
						queueSize = v["Publishing"]["queueSize"]
						queueDiscardOldest = v["Publishing"]["queueDiscardOldest"]
						if "filter" in v["Publishing"].keys():
							filter = v["Publishing"]["filter"]
						else:
							filter = "None"
						nodes_to_monitor_s.append({"node":var, "subId":subId, "samplingInterval":samplingInterval, "monitoringMode":monitoringMode, "queueSize":queueSize, "queueDiscardOldest":queueDiscardOldest, "filter":filter})
					except:
						print("\nErrors in Monitored item parameters!")
						wrong_json = True
					
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
	return nodes_to_handle, subscriptions_to_handle

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
	
def check_servers_status(servers, clients_list, nodes_to_handle, servers_subs, security_policies_uri, subscriptions_to_handle, kafka_producer_info, closing_event):
	print("\nCheck_servers_status started!")
	while(not closing_event.is_set()):			
		for i, s in enumerate(servers):
			try:
				# "ns=0;i=2259" is ServerStatus node
				clients_list[i].get_node("ns=0;i=2259").get_data_value()
			except:
				# check if previously working servers are still up
				if s["working"] != False:
					print(f"\nERROR: SERVER {s['address']} IS DOWN!")
					s["working"], clients_list[i], servers_subs[i] = False, None, None
					
				# check if previously down servers are up now 
				if s["working"] == False:
					# best_endpoint_selection and client_connection  
					client = client_connection(i, s, security_policies_uri, printable=False)
					if client != None: 
						clients_list[i] = client
						s["working"] = True
						print(f"\nCONNECTED to Server {s['address']}!")				
						# sub and monitored_items creation 
						sub_and_monitored_items_service(servers, clients_list, nodes_to_handle, subscriptions_to_handle, servers_subs, kafka_producer_info, closing_event)
						
		# this way we remove wrong nodes from NEW servers and it should remove a node if exists no more (has been deleted).
		removing_invalid_nodes(clients_list, servers, nodes_to_handle)	
		sleep(10)
	
def servers_connection(servers, security_policies_uri, nodes_to_handle, servers_subs, subscriptions_to_handle, kafka_producer_info, closing_event):
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
	
	check_servers_status_thread = threading.Thread(target=check_servers_status, args=(servers, clients_list, nodes_to_handle, servers_subs, security_policies_uri, subscriptions_to_handle, kafka_producer_info, closing_event,))
	check_servers_status_thread.start()
	return clients_list, check_servers_status_thread

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
					clients_list[i].get_node(n["node"]).get_data_value()	
				except:
					print(f"\n\nWARNING: Node {n['node']} of server {s['address']} for Polling service removed because it's not in address space.\n\n")
					nodes_to_handle[i][0].remove(n)				

class SubHandler(object):
	def __init__(self, server_url, kafka_producer_info):
		self.server_url = server_url
		self.kafka_producer_info = kafka_producer_info
		
	def datachange_notification(self, node, val, data):
		try:
			notification = {"server":self.server_url, "node":node.nodeid.to_string(), "value":str(val), "status":data.monitored_item.Value.StatusCode.name, "sourceTimestamp":str(data.monitored_item.Value.SourceTimestamp), "serverTimestamp":str(data.monitored_item.Value.ServerTimestamp)}
			print(dedent(f"""
				Time: {datetime.now()} - NOTIFICATION
				Server: {notification['server']}
				Node: {notification['node']}
				Value: {notification['value']} 
				StatusCode: {notification['status']} 
				SourceTimestamp: {notification['sourceTimestamp']} 
				ServerTimestamp: {notification['serverTimestamp']}
				"""))		
			
			publish_message(self.kafka_producer_info, "node", notification)
		except:
			print("\nError with monitored item notification!")				

def polling_function(pollingRate, nodes, clients_list, kafka_producer_info, closing_event):
	while(not closing_event.is_set()):		
		for n in nodes:
			try:
				parameters = ReadParameters()
				parameters.MaxAge= n["properties"]["maxAge"]	# da JSON
				#Source(0), Server(1), Both(2), Neither(3), Invalid(4)
				parameters.TimestampsToReturn=TimestampsToReturn[n["properties"]["timestampsToReturn"]]
			except: 
				parameters = ReadParameters()
			nodeToRead = ReadValueId()
			nodeToRead.AttributeId = 13 				#Value
			nodeToRead.NodeId = NodeId().from_string(n["node"])
			parameters.NodesToRead = [nodeToRead]
			
			try:						
				data_v = clients_list[n["server_idx"]].uaclient.read(parameters)[0]
				data_to_send = {"server":clients_list[n["server_idx"]].server_url.netloc, "node":n["node"], "value":str(data_v.Value.Value), "status":data_v.StatusCode.name, "sourceTimestamp":str(data_v.SourceTimestamp), "serverTimestamp":str(data_v.ServerTimestamp)}
				
				publish_message(kafka_producer_info, "node", data_to_send)
				
				print(dedent(f"""
				Time: {datetime.now()}
				Server: {data_to_send['server']}
				Node: {data_to_send['node']}
				Value: {data_to_send['value']} 
				StatusCode: {data_to_send['status']} 
				SourceTimestamp: {data_to_send['sourceTimestamp']} 
				ServerTimestamp: {data_to_send['serverTimestamp']}
				"""))		
				
			except:
				# this could be true only if something happens to server or a node has been removed.
				print(f"\nDetected a problem with node {n} from this server.")	
		for index in range(int(pollingRate/10)):
			sleep(0.01)				
	

def polling_service (servers, clients_list, nodes_to_handle, kafka_producer_info, closing_event):
	# READ SERVICE e MONITORED ITEMS notifications function
	nodes_for_pollingRate = {}
	for i, s in enumerate(servers): 	
		#print(f"\n\n{'-'*10} SERVER: {servers[i]['address']} {'-'*10}")
		if s["working"]!= False:		
			#print("\nREAD service: ")
			if len(nodes_to_handle[i][0]) > 0:				
				for n in nodes_to_handle[i][0]:   # nodes_to_read_s=[n,]  n={ nodo ;property}
					try:
						pollingRate = n["properties"]["pollingRate"]
					except:
						pollingRate = 10
				
					n["server_idx"] = i
					
					if pollingRate in nodes_for_pollingRate:						
						nodes_for_pollingRate[pollingRate].append(n)
					else:
						nodes_for_pollingRate[pollingRate] = list()	
						nodes_for_pollingRate[pollingRate].append(n)
			else:
				print("NO nodes to read for this server!")
		else:
			print("Server is DOWN!")
			
	polling_threads = list()
	for pollingRate in nodes_for_pollingRate:
		# dobbiamo passare alla funzione i nodi.
		#for nodes in nodes_for_pollingRate[pollingRate]:
		x = threading.Thread(target=polling_function, args=(pollingRate, nodes_for_pollingRate[pollingRate], clients_list, kafka_producer_info, closing_event,))
		polling_threads.append(x)
		x.start()
				
	return polling_threads
	
	
def sub_monitored_items_creation(sub, monitored_items):
	params = ua.CreateMonitoredItemsParameters()
	params.SubscriptionId = sub.subscription_id
	params.ItemsToCreate = monitored_items
	params.TimestampsToReturn = ua.TimestampsToReturn.Both

	# insert monitored item into map to avoid notification arrive before result return
	# server_handle is left as None in purpose as we don't get it yet.
	with sub._lock:
		for mi in monitored_items:
			data = SubscriptionItemData()
			data.client_handle = mi.RequestedParameters.ClientHandle
			data.node = Node(sub.server, mi.ItemToMonitor.NodeId)
			data.attribute = mi.ItemToMonitor.AttributeId
			#TODO: Either use the filter from request or from response. Here it uses from request, in modify it uses from response
			data.mfilter = mi.RequestedParameters.Filter
			sub._monitoreditems_map[mi.RequestedParameters.ClientHandle] = data
	results = sub.server.create_monitored_items(params)
	mids = []
	# process result, add server_handle, or remove it if failed
	with sub._lock:
		for idx, result in enumerate(results):
			mi = params.ItemsToCreate[idx]
			if not result.StatusCode.is_good():
				del sub._monitoreditems_map[mi.RequestedParameters.ClientHandle]
				mids.append(result.StatusCode)
				continue
			data = sub._monitoreditems_map[mi.RequestedParameters.ClientHandle]
			data.server_handle = result.MonitoredItemId
			mids.append(result.MonitoredItemId)
	return mids, results

	
def create_monitored_items(sub, dirty_nodes, attr):
	mirs = []
	for dirty_node in dirty_nodes:
		try:
			node_var = Node(sub.server, NodeId().from_string(dirty_node["node"]))
			rv = ua.ReadValueId()
			rv.NodeId = node_var.nodeid
			rv.AttributeId = attr
			# rv.IndexRange //We leave it null, then the entire array is returned
			mparams = ua.MonitoringParameters()
			with sub._lock:
				sub._client_handle += 1
				mparams.ClientHandle = sub._client_handle
			mparams.SamplingInterval = dirty_node["samplingInterval"]
			mparams.QueueSize = dirty_node["queueSize"]
			mparams.DiscardOldest = dirty_node["queueDiscardOldest"] == "True"
			
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
			
			if mfilter:
				mparams.Filter = mfilter	
			
			mir = ua.MonitoredItemCreateRequest()
			mir.ItemToMonitor = rv
			mir.MonitoringMode = ua.MonitoringMode.Disabled if dirty_node["monitoringMode"] == "Disabled" else ua.MonitoringMode.Sampling if dirty_node["monitoringMode"] == "Sampling" else ua.MonitoringMode.Reporting
			mir.RequestedParameters = mparams
			mirs.append(mir)
		except:
			print("\nUnable to create monitored item for node {dirty_node['node']}")		
		
	mids, results = sub_monitored_items_creation(sub, mirs)
	
	for i, r in enumerate(results):
		if dirty_node["samplingInterval"] != r.RevisedSamplingInterval or dirty_node["queueSize"] != r.RevisedQueueSize:
			print(f"\nServer has changed monitored item properties for node {dirty_node['node']}")
			print(f"Monitored item id: {r.MonitoredItemId}")
			print(f"Revised SamplingInterval: {r.RevisedSamplingInterval}")
			print(f"Revised QueueSize: {r.RevisedQueueSize}")
			print(f"Filter Result: {r.FilterResult}\n")
		
	return mids	
					
def sub_and_monitored_items_service(servers, clients_list, nodes_to_handle, subscriptions_to_handle, servers_subs, kafka_producer_info, closing_event):		
	print(f"\nSUBSCRIPTIONS CREATION...")
	print(f"\n{'-'*60}")
	print(f"\n{'*'*60}")
	for i, s in enumerate(servers):
		print(f"\nSERVER {s['address']}")
		if s["working"] != False and servers_subs[i]==None and len(nodes_to_handle[i][1]) > 0:
			# CREATE SUBSCRIPTIONS	
			servers_subs[i] = []							
			if subscriptions_to_handle[i] != None:
				# create a subscription only if there are nodes that require it.
				subs_ids_to_create = set([node["subId"] for node in nodes_to_handle[i][1]])
				print("\nTrying to create following subscriptions: ", subs_ids_to_create)
			
				for server_sub_to_handle in subscriptions_to_handle[i]:
					if server_sub_to_handle["subId"] not in subs_ids_to_create:
						print(f"\nNo nodes to monitor for subscription {server_sub_to_handle['subId']}.")
						continue										
					try: 
						sub_params = CreateSubscriptionParameters()
						sub_params.RequestedPublishingInterval = server_sub_to_handle["publishingInterval"]
						sub_params.RequestedLifetimeCount = server_sub_to_handle["lifetimeCount"]
						sub_params.RequestedMaxKeepAliveCount = server_sub_to_handle["maxKeepAliveCount"]
						sub_params.MaxNotificationsPerPublish = server_sub_to_handle["maxNotificationsPerPublish"]
						sub_params.PublishingEnabled = server_sub_to_handle["publishingEnabled"]
						sub_params.Priority = server_sub_to_handle["priority"]
						sub = clients_list[i].create_subscription(sub_params, SubHandler(clients_list[i].server_url.netloc, kafka_producer_info))
						print(f"\nSubscription {server_sub_to_handle['subId']} created!")
					except:
						print(f"\nCannot create subscription {server_sub_to_handle['subId']} for server {s['address']}! All nodes to monitor will be skipped.")
						continue
						# create monitored items
					try:
						nodes_to_monitor = [node for node in nodes_to_handle[i][1] if node["subId"]==server_sub_to_handle["subId"]]
						handle = create_monitored_items(sub, nodes_to_monitor, ua.AttributeIds.Value)	
						servers_subs[i].append({"subId":server_sub_to_handle["subId"], "sub":sub, "handle":handle})							
					except:
						print(f"\nUnable to create monitored items for subscription {server_sub_to_handle['subId']}. Subscription Deleting...")
						sub.delete()
			else:
				print(f"\nNo subscription for server {s['address']}")
		print(f"\n{'*'*60}")		
	print(f"\n{'-'*60}")
	print("\nRECEIVING NODES UPDATES...")
	print(f"\n{'-'*60}")
	
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