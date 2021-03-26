import socket
import select
import sys
import pickle
import errno
import time
from time import sleep
from backend import Node

ip = "127.0.0.1"
port = 9912
bootstrap_ip = "127.0.0.1"
bootstrap_port = 9910
recv_length = 102400

node = Node(ip, port, False)

def create_server_socket():
	# create a socket that will be used by other nodes when they first connect
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# reuse adress for debugging only
	server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

	server_socket.bind((ip, port))
	# enable the server to accept connections
	server_socket.listen()

	# list of sockets for select.select()
	node.add_socket(server_socket)

def connect_to_dht():
	# create a socket and use it to connect to bootstrap server
	client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# connect to bootstrap server
	client_socket.connect((bootstrap_ip, bootstrap_port))
	node.set_counter()

	# set connection to non-blocking state, so .recv() call won't block, just return some exception we'll handle
	# if blocking was set to True that would mean wait until something is received
	client_socket.setblocking(False)

	# inform bootstrap of port
	msg = [[node.get_id(), node.get_counter(), 0], [node.get_ip_address(), node.get_port()]]
	msg = pickle.dumps(msg, -1)
	client_socket.send(msg)

	# three cases:
	# two other nodes will try connect with this server
	# one other node and the bootstrap will try to connect with this server
	# only the bootstrap will try to connect with this server
	while node.get_predecessor() == None or node.get_successor() == None:
		# check server socket for other nodes and client socket for bootstrap
		read_sockets, _, exception_sockets = select.select(node.get_sockets()+[client_socket], [], node.get_sockets()+[client_socket])
		# iterate over notified sockets
		for notified_socket in read_sockets:
			node.set_counter()
			# first check for bootstrap
			if notified_socket == client_socket:
				successor_id = node.compute_id(bootstrap_ip, bootstrap_port)
				# wait until info about predecessor and successor arrives
				[_, [_, succ, [answer_k, answer_consistency]]] = receive(client_socket)
				# set consistency and k
				node.set_consistency(answer_consistency)
				node.set_k(answer_k)
				node.join([bootstrap_ip, bootstrap_port, client_socket], succ)
			# then check for other servers, they will try to connect to this one
			else:
				# the returned value is a pair (conn, address) where conn is a new socket object usable to send and
				# receive data on the connection, and address is the address bound to the socket on the other end of the connection.
				predecessor_socket, _ = node.get_sockets()[0].accept()
				# wait until info about predecessor and successor arrives
				[_, [pred, succ, [answer_k, answer_consistency]]] = receive(predecessor_socket)
				# set consistency and k
				node.set_consistency(answer_consistency)
				node.set_k(answer_k)
				# check if bootstarp is the successor
				if succ[0] == bootstrap_ip and succ[1] == bootstrap_port:
					node.join([pred[0], pred[1], predecessor_socket], succ, client_socket)
				else:
					node.join([pred[0], pred[1], predecessor_socket], succ)

# function for receiving a message from a socket
def receive(socket):
	while True:
		try:
			msg = socket.recv(recv_length)
			msg = pickle.loads(msg)
			# if we received no data, client gracefully closed a connection
			if not msg:
				return False
			return msg
		except IOError as e:
			# This is normal on non blocking connections - when there are no incoming data error is going to be raised
			# Some operating systems will indicate that using AGAIN, and some using WOULDBLOCK error code
			# We are going to check for both - if one of them - that's expected, means no incoming data, continue as normal
			# If we got different error code - something happened
			if e.errno != errno.EAGAIN and e.errno != errno.EWOULDBLOCK:
				print('Reading error: {}'.format(str(e)))
				sys.exit()
			# we just did not receive anything
			continue
		except Exception as e:
			print("Some error occured, probably some node didn't depart correctly: ".format(str(e)))
			print(socket.fileno())
			print(str(e))
			sys.exit()

"""
	This function runs after the node is connected to the DHT, and needs to be updated on the
	data it must have. It needs to connect to the successor and (possibly) shift the data
	left-wise.
"""
def get_data():
	succ = node.get_successor()
	msg = [[node.get_id(), node.get_counter(), 10], [node.get_id(), {}, {}, node.get_id()]]
	msg = pickle.dumps(msg, -1)
	succ[2].send(msg)
	return


def main_loop():

	file = open("insert_1.txt")
	insert_lines = file.readlines()
	for i in range(len(insert_lines) - 1):
		insert_lines[i] = insert_lines[i][:-2]

	for i in range(len(insert_lines)):
	    insert_lines[i] = insert_lines[i].split(",")
	insert_lines.reverse()
	file.close()
	
	
	# file = open("query_1.txt")
	# query_lines = file.readlines()
	# for i in range(len(query_lines) - 1):
	# 	query_lines[i] = query_lines[i][:-2]
	# query_lines.reverse()
	# file.close()
	
	# file = open("requests_1.txt")
	# request_lines = file.readlines()
	# for i in range(len(request_lines) - 1):
	# 	request_lines[i] = request_lines[i][:-2]

	# for i in range(len(request_lines)):
	#     request_lines[i] = request_lines[i].split(",")
	# request_lines.reverse()
	# file.close()

	insert_time_start = time.mktime(time.struct_time((2021,3,26,5,33,00,4,85,0)))
	# query_time_start = time.mktime(time.struct_time((2021,3,26,4,55,00,4,85,0)))
	# request_time_start = time.mktime(time.struct_time((2021,3,26,5,06,00,4,85,0)))

	while True:
		sleep(0.01)
		# iterate over all sockets, choose those that have been activated, set time interval to 0 for non-blocking
		read_sockets, _, exception_sockets = select.select(node.get_sockets(), [], node.get_sockets(), 0)

		# iterate over notified ones
		for notified_socket in read_sockets:
			print()
			if notified_socket == node.get_sockets()[0]:
				# the returned value is a pair (conn, address) where conn is a new socket object usable to send and
				# receive data on the connection, and address is the address bound to the socket on the other end of the connection.
				peer_socket, peer_address = node.get_sockets()[0].accept()
				# wait for info on port
				[[peer_id, _, code], info] = receive(peer_socket)
				if code == 12:
					[peer_ip, peer_port, message] = info
					print("I got a message of ACK from ip",peer_ip,",port",peer_port,": counter =", message)
					node.set_end()
					if peer_socket not in node.get_sockets():
						peer_socket.close()
				elif code == 3:
					[_, succ] = info
					node.update_dht(succ[0], succ[1], succ[2], code, peer_socket)
				else:
					pred = info
					print("just received a new connection from", peer_id, "with info", pred)
					node.update_dht(pred[0], pred[1], peer_id, code, peer_socket)
					print()
			elif notified_socket not in node.get_sockets():
				continue
			else:
				[[peer_id, count, code], info] = receive(notified_socket)
				# print(code, info)
				# check for new successor
				if code == 0 or code == 2 or code == 3:
					[_, succ] = info
					node.update_dht(succ[0], succ[1], succ[2], code)
				#insert code
				elif code == 4:
					[key,value, peer_ip_address, peer_port, counter] = info
					node.insert(key,value,peer_ip_address, peer_port, counter)
				#delete code
				elif code == 5:
					[key] = info
					node.delete(key)
				#insert replica code
				elif code == 6:
					[key, value, peer_ip, peer_port, peer_id, currentk, peer_ip_address, old_peer_port, counter] = info
					node.replica_insert(key, value, peer_ip, peer_port, peer_id, currentk, peer_ip_address, old_peer_port, counter)
				#delete replica code
				elif code == 7:
					[key, peer_ip, peer_port, peer_id, currentk] = info
					node.replica_delete(key, peer_ip, peer_port, peer_id, currentk)
				#query code
				elif code == 8:
					[key, starting_node_ID ,peer_ip_address, peer_port, counter, round_trip, found_number] = info
					node.query(key, starting_node_ID ,peer_ip_address, peer_port, counter, round_trip, found_number)
				#update data on predecessor departing:
				elif code == 9:
					[sent_data, send_key, departing_node_id] = info
					node.update_data_on_depart(sent_data, send_key, departing_node_id)
				elif code == 10:
					[new_node_ID, data_to_be_updated, counters_to_be_updated, message_sender_ID] = info
					print(info)
					node.update_data_on_join(new_node_ID, data_to_be_updated, counters_to_be_updated, message_sender_ID)
				# overlay code:
				elif code == 11:
					[list_of_nodes] = info
					# print(list_of_nodes)
					node.overlay(list_of_nodes)
				# ACK code
				elif code == 12:
					[peer_ip, peer_port, message] = info
					print("I got a message of ACK from ip",peer_ip,",port",peer_port,": counter =", message)
					node.set_end()
			print()

		#Expirements
		# if time == smth:
		# 	node.set_start()
		# pops
		# if its last item so something interesting
		# i.e. track its counter.

		# check for input, set time interval to 0 for non-blocking
		input = select.select([sys.stdin], [], [], 0)[0]
		if input:
			print()
			value = sys.stdin.readline().rstrip()
			if str(value) == "depart":
				node.depart()
				return
			elif str(value).lower().startswith("insert"):
				temporary = str(value).split(',')
				if (len(temporary) > 2):
					key = temporary[1].strip()
					some_value = temporary[2].strip()
					node.insert(key,some_value,node.get_ip_address(),node.get_port(),node.get_counter())
				else:
					print("Wrong Input")
			elif str(value).lower().startswith("delete"):
				temporary = str(value).split(',')
				if (len(temporary) > 1):
					key = temporary[1].strip()
					node.delete(key)
			elif str(value).lower().startswith("debug"):
				print(node.get_predecessor())
				print(node.get_successor())
				for sock in node.get_sockets():
					print(sock)
			elif str(value).lower().startswith("query"):
				temporary = str(value).split(',')
				if (len(temporary) > 1):
					starting_node_ID = node.get_id()
					key = temporary[1].strip()
					node.query(key, starting_node_ID,node.get_ip_address(),node.get_port(),node.get_counter())
				else:
					print("Wrong Input")
			elif str(value).lower().startswith("overlay"):
				succ = node.get_successor()
				if succ:
					starting_node_ID = node.get_id()
					list_of_nodes = [starting_node_ID]
					node.overlay(list_of_nodes)
				else:
					print("The Chord is just me! Add more nodes please!")
			elif str(value).lower().startswith("help"):
				print("""
Welcome to ToyChord's 1.0 help!

The basic functionalities of the ToyChord CLI include the following:

• \033[4minsert, <key> , <value>\033[0m:
	This function when called, inserts a (key, value) pair, where key is the
	name of the song, and value a string (that supposedly returns the node
	that we must connect to, in order to download said song). Example usage:

	insert, Like a Rolling Stone, 1

• \033[4mdelete, <key>\033[0m:
	This function when called, deletes a the data related to key, where key
	is the name of the song. Example usage:

	delete, Like a Rolling Stone

• \033[4mquery, <key>\033[0m:
	This function when called, looks up a key in the DHT, and if it exists,
	it returns the corresponding value, from the node that is responsible for
	this key. You can query the special character "*", and have every
	<key,value> pairs of every node returned. Example usages:

	query, Like a Rolling Stone
	query, *

• \033[4mdepart\033[0m:
	This function gracefully removes a node from the DHT, allowing the Chord
	to tidily shut down its connections with the other nodesand then remove
	it from the system.

	depart

• \033[4moverlay\033[0m:
	This function prints out the nodes that exist in the DHT (each node is
	represented by its ID), in a manner that shows the order by which the nodes
	are connected. Example usage:

	overlay

• \033[4mhelp\033[0m:
	This function prints out this message, assisting you with efficiently using
	this CLI. Example usage:

	help
""")
			else:
				print(f"You entered: {value}, did you make a mistake?")
			print()


		# # check all sockkets to be closed if other closed them close them aswell
		if time.time() >= insert_time_start:
			# start inserting
			if insert_lines:
				key,value = insert_lines.pop()
			else:
				insert_time_start += 100000000
			node.insert(key,value,node.get_ip_address(),node.get_port(),node.get_counter())

		# if time.time() >= query_time_start:
		# 	# start quering
			# if query_lines:
			# 	key = query_lines.pop()
			# else:
			# 	query_time_start += 100000000
		# 	starting_node_ID = node.get_id()
		# 	node.query(key, starting_node_ID,node.get_ip_address(),node.get_port(),node.get_counter())
		
		# if time.time() >= request_time_start:
		# 	# start requesting
			# request = request_lines.pop()
			# if request_lines:
			# 	request = request_lines.pop()
			# else:
			# 	request_time_start += 100000000
		# 	# if 2 terms then it's a query
		# 	if len(request) == 2:
		# 		_, key = request
		# 		starting_node_ID = node.get_id()
		# 		node.query(key, starting_node_ID,node.get_ip_address(),node.get_port(),node.get_counter())
		# 	# if 3 terms then it's an insert:
		# 	elif len(request) == 3:
		# 		_, key, value = request
		# 		key,value = insert_lines.pop()
		# 		node.insert(key,value,node.get_ip_address(),node.get_port(),node.get_counter())

if __name__ == '__main__':
	for i, arg in enumerate(sys.argv):
		if i == 1:
			port = int(arg)
	create_server_socket()
	connect_to_dht()
	sleep(1)
	print()
	print("Hello there (General Kenobi)! I have just joined! Gib data pls?")
	print()
	get_data()
	main_loop()
