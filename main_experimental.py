import sys, time
import docker
import subprocess
import ldap
import socket
import requests,json

from dateutil.parser import parse

#Global docker client
client = docker.from_env()

#Initiate swarm and deploy based on given compose file
def single_host_swarm(stack_name):
	try:
		client.swarm.init()
	except docker.errors.APIError, e:
		print e

	subprocess.call(['docker', 'stack', 'deploy', '-c', 'docker-compose.yml', stack_name])

	print "Created swarm and stack"

	#Generate the worker token
	process = subprocess.Popen(['docker', 'swarm', 'join-token', '--quiet', 'worker'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out = process.communicate()
	worker_join_token = out[0]

	#Deploy a global Cadvisor service for stat collection
	mount_list = ['/:/rootfs:ro','/var/run:/var/run:rw', '/sys:/sys:ro', '/var/lib/docker/:/var/lib/docker:ro', '/dev/disk/:/dev/disk:ro']
	endpoint_arg = docker.types.EndpointSpec(ports={8080:8080})
	mode_arg = docker.types.ServiceMode(mode='global')

	client.services.create(image='google/cadvisor:latest', mode=mode_arg, endpoint_spec=endpoint_arg, mounts=mount_list, name='cadvisor')

	#Add labels to node and add placement preferences for services to stay in node
	node_list = client.nodes.list(filters={'role': 'manager'})
	node_spec = {'Availability': 'active', 'Role': 'manager', 'Labels': {'local' : 'true'}}
	node_list[0].update(node_spec)

	service_list = client.services.list()
	for service in service_list:
		if 'cadvisor' not in service.name:
			subprocess.call(['docker', 'service', 'update', '--constraint-add', 'node.hostname==georg-Inspiron-3537', \
							'--update-order', 'start-first', service.id])

	return worker_join_token



#Cleanup of all open services and docker swarm	
def cleanup(stack_name):
	subprocess.call(['docker', 'stack', 'rm', stack_name])

	client.swarm.leave(force=True)


#Open Connection to LDAP server to retrieve devices. Returns an ldap object
def ldap_connection(server_ip):
	try:
		l = ldap.open(server_ip)
	except ldap.LDAPError, e:
		print e

	return l

#Get the IP of devices in local server. Requires ldap object and returns a list of IP
def get_devices(l):

	ip_list=[]

	baseDN = "ou=devices,dc=uth1vm,dc=edu"
	searchScope = ldap.SCOPE_SUBTREE
	retrieveAttributes = None
	#searchFilter = "cn=uth2"
	searchFilter = "cn=*"

	try:
		ldap_result_id = l.search(baseDN, searchScope, searchFilter, retrieveAttributes)
		result_set = []
		while 1:
			result_type, result_data = l.result(ldap_result_id, 0)
			if (result_data == []):
				break
			else:
				if result_type == ldap.RES_SEARCH_ENTRY:
					result_set.append(result_data)
		#print result_set
	except ldap.LDAPError, e:
		print e

	#ldap search returns a list with 2 elements. The dn and a dicti of obj class + attr.
	#Retrieve IP and specs and make a dictionary with IP as keys and Dictionary of specs as value
	#Format is {'ip':{'cores': value, 'cpu': value, 'memory': value}}

	device_dict = {}
	

	for entry in result_set:
		temp_spec_list = list()
		
		for item in entry:
			try:
				ip = item[1]['ipHostNumber'][0]
				temp_specs = item[1]['description'][0]
			except KeyError:
				print "Key not found"
		
		temp_specs = temp_specs.split(',')
		
		for item in temp_specs:
			temp_spec_list.append(item.split('='))

		device_dict[ip] = {}
		for item in temp_spec_list:
			device_dict[ip][item[0]] = int(item[1])

	return device_dict


#Pick devices to join the swarm based on latency and number of services in compose file
def avg_latency(device_dict):
	
	#measure latency for each device
	for device_ip in device_dict:
		#get ping values
		process = subprocess.Popen(["fping", '-C3', '-q', device_ip], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out = process.communicate()
		output = out[1]

		#extract values in list
		list_output = output.split()
		del list_output[:2]

		#Calculate average latency
		sum_add = 0.0
		for value in list_output:
			if value != '-':
				sum_add = sum_add + float(value)
			else:
				#acount for packet lost (currently add 1000ms if packet is lost)
				sum_add = sum_add + 4000


		avg = sum_add/len(list_output)

		device_dict[device_ip]['latency'] = avg

	return device_dict



#Send token to devices based on migration policy results
#Also label these nodes based on their hostname
def send_token(policy_res, token):

	ip_list = []
	node_ip_list = []

	node_list = client.nodes.list()
	for node in node_list:
		node_ip = node.attrs['Status']['Addr']
		node_ip_list.append(node_ip)


	#Extract unique IP from results
	for container in policy_res:
		if (policy_res[container] not in ip_list) and (policy_res[container] not in node_ip_list):
			#Only add IPs not already in swarm
			ip_list.append(policy_res[container])


	#send token to IP adresses in list
	for device_ip in ip_list:

		if device_ip != 'localhost':
			print '\n'
			print 'Sending token to ' + device_ip

			#Open socket and send token
			s = socket.socket()
			host = device_ip
			port = 8000

			s.connect((host, port))
			
			s.send("JOIN")
			response = s.recv(1024)

			if response == "REQ_token":
				
				#Send the modified token
				token_msg = "token." + token

				s.send(token_msg)

				ack_resp = s.recv(1024) 

				#Attempt to update nodes with hostname label here
				if ack_resp == "ACK":
					node_list = client.nodes.list()

					for node in node_list:
						if node.attrs['Status']['Addr'] == device_ip:

							node_hostname = node.attrs['Description']['Hostname']

							node_spec = {'Availability': 'active', 'Role': 'worker', 'Labels': {node_hostname : 'true'}}
							node.update(node_spec)

							print "Updated node " + node.attrs['Status']['Addr'] + " with hostname " + node_hostname


			#If the IP cant join the swarm run on local
			elif response == "BUSY":
				pass

			s.close()

#Get container/service stats from cadvisor
#Timestamps record the last minute, with about a second difference in each stamp
def get_stats(stat_dict, legacy_net):

	#Get the running containers on host
	container_list = client.containers.list()
	
	#Get machine info
	machine_url = 'http://localhost:8080/api/v1.3/machine'

	#If connection is denied, wait 1 second for container to go up
	#Break After 10 tries
	counter = 0
	while counter<10:
		try:
			r = requests.get(machine_url)
			break
		except requests.exceptions.ConnectionError, e:
			time.sleep(1)
			print str(e)
			counter += 1

	parsed_data = json.loads(r.text)

	host_memory_size = parsed_data['memory_capacity']

	#Loop through containers and get stats for each
	for container in container_list:

		if ('cadvisor' in container.name) or ('competent_raman' in container.name):
			print 'Skipped ' + container.name
			continue

		#Save old value in case of failure
		if container in legacy_net:
			old_kbytes_per_sec = legacy_net[container]
		else:
			old_kbytes_per_sec = 0

		print 'Gathering stats for ' + container.name

		url = 'http://localhost:8080/api/v1.3/docker/' + container.id

		#If connection is denied, wait 1 second for container to go up
		#Break After 10 tries
		counter = 0
		while counter<10:
			try:
				r = requests.get(url)
				#break
			except requests.exceptions.ConnectionError, e:
				time.sleep(1)
				print str(e)
				print "ERROR"
				counter += 1

			try:
				parsed_data = json.loads(r.text)
				break
			except ValueError, e:
				time.sleep(1)
				print str(e)
				counter += 1

		


		id_path = '/docker/' + container.id

		#List that holds the stats values for the container. Structure is [memload, KB/sec, cpuload]
		stat_list = []

		#Memory vars
		sum_of_memload = 0
		count_of_stamps = 0

		#Network vars
		net_data_flag = False

		#CPU vars
		cpu_data_flag = False

		#Loop through the cadvisor timestamps
		#We loop throught the whole last minute
		for stamp_id in range(0, 60):

			#Memory stats gathering
			try:
				sum_of_memload += parsed_data[id_path]['stats'][stamp_id]['memory']['usage']
				count_of_stamps += 1
			except IndexError:
				#print "Failed Memory time stamp is: " + str(stamp_id) + " for container: " + container.name
				pass

			#Network stats gathering
			#Loop backwards here only until we find the most recent data
			if not net_data_flag:
				try:
					rx_bytes = parsed_data[id_path]['stats'][59-stamp_id]['network']['rx_bytes']
					tx_bytes = parsed_data[id_path]['stats'][59-stamp_id]['network']['tx_bytes']

					net_time_delta = parse(parsed_data[id_path]['stats'][stamp_id]['timestamp']) \
								 - \
								 parse(parsed_data[id_path]['spec']['creation_time'])
					net_data_flag = True
				except IndexError:
					#print "Failed Network time stamp is: " + str(stamp_id) + " for container: " + container.name
					net_data_flag = False


			#CPU stats gathering
			#Get average usage in the last minute, or from latest timestamp to timestamp 0
			if not cpu_data_flag:
				try:
					cpu_delta = parsed_data[id_path]['stats'][59-stamp_id]['cpu']['usage']['total'] \
								- \
								parsed_data[id_path]['stats'][0]['cpu']['usage']['total']

					cpu_time_delta = parse(parsed_data[id_path]['stats'][59-stamp_id]['timestamp']) \
					 				 - \
					 				 parse(parsed_data[id_path]['stats'][0]['timestamp'])
					cpu_data_flag = True
				except IndexError:
					#print "Failed CPU time stamp is: " + str(stamp_id) + " for container: " + container.name
					cpu_data_flag = False


		#Memory after loop operations
		average_memload = sum_of_memload/count_of_stamps
		average_memload_percentage = 100 * (float(average_memload) / float(host_memory_size))
		
		stat_list.append(average_memload_percentage)
		#print "For container " + container.name + " average memory usage is " + str(average_load_percentage) + '%'

		#Network after loop operations
		if net_data_flag:
			net_time_delta_sec = net_time_delta.total_seconds()
			bytes_per_sec = float(tx_bytes + rx_bytes) / float(net_time_delta_sec)
			kbytes_per_sec = bytes_per_sec / float(1024)
			stat_list.append(kbytes_per_sec)
		#Use old data if all timestamps failed
		else:
			print "Using old net data"
			stat_list.append(old_kbytes_per_sec)

		#print "Container " + container.name + " uses : " + str(kbytes_per_sec) + " KB per sec"

		#CPU after loop operations
		cpu_time_delta_sec = cpu_time_delta.total_seconds()
		cpu_load_percentage = 100 * (float(cpu_delta) / float(1000000000 * cpu_time_delta_sec))

		stat_list.append(cpu_load_percentage)
		#print "For container " + container.name + " cpu percentage is: " + str(cpu_load_percentage)

		stat_dict[container.id] = stat_list

	return stat_dict


#Prune devices that do not meet criteria, as to not consider them in further calculations.
#Currently this refers to lower cpu specs than localhost
def prune_devices(device_dict):

	prune_list = []

	for ip in device_dict:
		
		if (device_dict[ip]['cores']*device_dict[ip]['cpu']) < (device_dict['localhost']['cores']*device_dict['localhost']['cpu']):
			
			prune_list.append(ip)

	for ip in prune_list:

		del device_dict[ip]

	return device_dict

#Determine best device to accommodate each service
#Based on the stats gathered and device characteristics
#Container dict format: Key=Container.ID and Value=[memload, KB/sec, cpuload]
def compute_migration(device_dict, stat_dict):

	#TCP window size
	window_size = 65535

	final_score = {}

	#local_cpu = device_dict['localhost']['cpu']*device_dict['localhost']['cores']
	local_cpu = device_dict['localhost']['cpu']

	for container in stat_dict:
		
		container_obj = client.containers.get(container)

		print "\n"
		print container_obj.name

		#Check for the cadvisor service so that it isnt included in calculations
		if "cadvisor" not in container_obj.name and stat_dict[container]:
			
			score = 0
			final_score[container] = 'localhost'

			for ip in device_dict:

				#Apply memory filter before calculations
				if stat_dict[container][0] <= device_dict[ip]['memory']:

					#Calculate score based on formula
					core_diff = device_dict[ip]['cores'] / device_dict['localhost']['cores']

					#dest_cpu = device_dict[ip]['cpu']*device_dict[ip]['cores']
					dest_cpu = device_dict[ip]['cpu']*core_diff

					cpu_increase = ((dest_cpu - local_cpu) / local_cpu) * 100

					comm_overhead = (stat_dict[container][1] / (window_size / device_dict[ip]['latency'])) * 100

					temp_score = (stat_dict[container][2] * cpu_increase) - comm_overhead

					print "Score for ip " + ip + " is " + str(temp_score)

					if temp_score > score :
						score = temp_score

						final_score[container] = ip

	return final_score


#Get nodes based on corresponding IPs
#Add Labels for appropriate services
#Add placement-pref to services
def update_swarm(policy_res, old_policy):

	service_list = client.services.list()
	node_list = client.nodes.list()

	for container in policy_res:

		local_update_start = time.time()

		if container not in old_policy:
			old_policy[container] = 'localhost'

		if policy_res[container] != old_policy[container]:

			print "Update Container " + container + " from " + policy_res[container] + " to " + old_policy[container] 

			container_obj = client.containers.get(container)

			#Get service id from container attributes
			service_id = container_obj.attrs['Config']['Labels']['com.docker.swarm.service.id']
			device_ip = policy_res[container]

			service_obj = client.services.get(service_id)

			#If the destination IP is localhost apply local labels
			if device_ip == 'localhost':
				#Remove possible pre-existing placement pref
				#And add new
				try:
					#pref = service_obj.attrs['Spec']['TaskTemplate']['Placement']['Preferences'][0]['Spread']['SpreadDescriptor']
					constr = service_obj.attrs['Spec']['TaskTemplate']['Placement']['Constraints'][0]
					subprocess.call(['docker', 'service', 'update', '--constraint-rm', constr, \
									'--constraint-add', 'node.hostname==georg-Inspiron-3537', service_obj.id])
				except KeyError, e:
					print "KeyError on pref" + str(e)
					subprocess.call(['docker', 'service', 'update', '--constraint-add', 'node.hostname==georg-Inspiron-3537', service_id])
				
				
			
			#If the destination IP is remote
			else:
				for node in node_list:
					node_ip = node.attrs['Status']['Addr']

					print "Node IP is " + node_ip + " device IP " + device_ip 

					#Get the correct node according to IP
					if node_ip == device_ip:

						'''
						#Label the node with corresponding service id
						node_spec = {'Availability': 'active', 'Role': 'worker', 'Labels': {service_id : 'true'}}
						node.update(node_spec)

						print "Updated node " + node_ip
						'''

						node_hostname = node.attrs['Description']['Hostname']
						print "Will update service " + service_obj.name + " with hostname " + node_hostname

						#Remove possible pre-existing placement pref and
						#Add the new placement pref in one step
						try:
							#pref = service_obj.attrs['Spec']['TaskTemplate']['Placement']['Preferences'][0]['Spread']['SpreadDescriptor']
							constr = service_obj.attrs['Spec']['TaskTemplate']['Placement']['Constraints'][0]
							subprocess.call(['docker', 'service', 'update', '--constraint-rm', constr, \
									'--constraint-add', 'node.hostname==' + node_hostname, service_obj.id])
							#!!!!CHeck what preference to remove!!!!
						except KeyError, e:
							print "KeyError on pref"
							print e

							#Add the new placement pref
							subprocess.call(['docker', 'service', 'update', '--constraint-add', 'node.hostname==' + node_hostname, \
											service_obj.id])

		else:
			print "No update " + container + " from " + policy_res[container] + " to " + old_policy[container] 

		local_update_end = time.time()
		local_update_res = local_update_end - local_update_start

		print "UPDATED SERVICE " + service_obj.name + ' TIMER : ' + str(local_update_res)

	return

def health_check(device_dict):

	service_list = client.services.list()

	node_list = client.nodes.list()

	for service in service_list:

		#Hardcoded values, corresponding to visualization container
		#Remove/Adjust
		if ('cadvisor' not in service.name) and ('competent_raman' not in service.name):

			print "Checking health for " + service.name

			try:

				constraint = service.attrs['Spec']['TaskTemplate']['Placement']['Constraints'][0]
				placement = constraint.split('==')[1]
			except KeyError, e:
				print e
				continue

			for node in node_list:
				#Find the node the service is placed
				if placement==node.attrs['Description']['Hostname']:
					#Check if down
					if node.attrs['Status']['State'] == 'down':

						print "Node " + node.attrs['Status']['State'] + ' is down'
						print 'Reverting to local'

						constr = service.attrs['Spec']['TaskTemplate']['Placement']['Constraints'][0]
						#reschedule on local
						subprocess.call(['docker', 'service', 'update', '--constraint-rm', constr, \
									'--constraint-add', 'node.hostname==georg-Inspiron-3537', service.id])

						#remove from device_dict
						node_ip = node.attrs['Status']['Addr']

						if node_ip in device_dict:
							del device_dict[node_ip]


	return device_dict









###########################################
#######           MAIN         ############
###########################################
if __name__ == "__main__":

	setup_start = time.time()

	#LDAP Server IP
	server_ip = "10.96.12.67"

	#Setup a swarm on a single host
	#Stack name taken from arguments, if not given generate one from the clock (!not guaranteed unique!)
	if len(sys.argv) > 1 :
		stack_name = sys.argv[1]
	else:
		stack_name = "Stack_" + str(int(time.time()))

	worker_token = single_host_swarm(stack_name=stack_name)

	
	#All ldap code is based on http://www.grotan.com/ldap/python-ldap-samples.html
	#Create an ldap connection and retrieve dict with devices and specs
	ldap_obj = ldap_connection(server_ip)

	setup_flag = False

	#Counter to measure time between calls
	time_counter = 0

	#Holder for legacy net values
	legacy_net = {}

	#Holder for old_policy
	old_policy = {}

	while True:

		#Get new devices every 2 minutes
		if time_counter%120 == 0:

			device_dict = get_devices(ldap_obj)

			print '\n'
			print 'DEVICES FROM LDAP'
			#print device_dict

			#Add local device info and then prune ineligible devices
			#Now Hardcoded. Could get info dynamically.
			device_dict['localhost'] = {'cores': 1, 'cpu': 1379, 'memory': 4096}
			device_dict = prune_devices(device_dict)

			print '\n'
			print 'PRUNED'
			print device_dict

			#Calculate latencies and add to device dictionary
			device_dict = avg_latency(device_dict)
			print '\n'
			print 'DEVICES WITH LATENCIES'
			print device_dict

			'''
			#For measuring, only local available
			device_dict = {}
			device_dict['localhost'] = {'cores': 4, 'cpu': 1379, 'latency': 0.04666666666666667, 'memory': 4096}
			print '\n'
			print "CHEATING"
			print device_dict
			'''

		#Get new stats every 5 seconds
		if time_counter%10 == 0:

			#Create dict of lists for container stats. Key=Container.ID and Value=[memload, KB/sec, cpuload]
			container_list = client.containers.list()
			stat_dict = {}

			for container in container_list:
				if ('cadvisor' in container.name) or ('competent_raman' in container.name):
					continue
				stat_dict[container.id] = list()

			#Setup sleep for cadvisor to go up
			#Should happen only once
			if not setup_flag:
				print '\n'
				print "Setting up Cadvisor"
				time.sleep(60)
				

				setup_end = time.time()
				setup_elapsed = setup_end - setup_start
				print '\n'
				print 'Setup Time: ' + str(setup_elapsed)
				raw_input("Setup Complete. Press Enter to continue")

			start_timer = time.time()

			stat_dict = get_stats(stat_dict, legacy_net)

			#print stat_dict

			for container in stat_dict:
				try:
					legacy_net[container] = stat_dict[container][1]
				except IndexError, e:
					print e

			print '\n'
			print 'CONTAINER NAME AND STATS'
			for key in stat_dict:
				stat_temp_cont = client.containers.get(key)
				
				if stat_dict[key]:
					print "For container " + stat_temp_cont.name + " stats are" \
							+ " memload: " + str(stat_dict[key][0]) \
							+ " net KB/s: " + str(stat_dict[key][1]) \
							+ " CPUload: " + str(stat_dict[key][2])
				else:
					print "For container " + stat_temp_cont.name + " stats are " + str(stat_dict[key])

			#Initialization of old policy values use in update_swarm
			#Based on stat dict containers
			if not setup_flag:
				for container in stat_dict:
					old_policy[container] = 'localhost'

				setup_flag = True

		#Compute migration every 10s
		if time_counter%10 == 0:

			print "\n"
			print "COMPUTING MIGRATION"
			#Compute ideal device for each container based on migration policy
			#Returns a dictionary where key=conainer.id, value=device IP
			policy_res = compute_migration(device_dict, stat_dict)

			print '\n'
			print 'MIGRATION POLICY RESULTS'
			for container in policy_res:
				policy_temp_cont = client.containers.get(container)
				print "For container " + policy_temp_cont.name + " destination is " + policy_res[container]

			
			#send token to the IP addresses from the policy results
			send_token(policy_res, worker_token)

			update_timer = time.time()

			res_update_timer = update_timer - start_timer

			print "UPDATE_SWARM TIMER : " + str(res_update_timer)

			#update swarm and services so that they are placed at the appropriate node
			update_swarm(policy_res, old_policy)
			print "\n"
			print "Swarm and services updated"

			end_timer = time.time()
			res_timer = end_timer - start_timer

			print "TIMER : " + str(res_timer)

			#Store old policy values
			if policy_res:
				old_policy = policy_res

			#Check node health every 2 seconds
			if time_counter%2 == 0:
				#Check if nodes are up
				device_dict = health_check(device_dict)
		
		time.sleep(1)
		time_counter = time_counter + 1

	cleanup(stack_name)
	print "Removed stack and swarm"


