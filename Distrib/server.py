import socket
import select
import sys
import pickle
import errno
from backend import Node

#ip = "192.168.0.2"
ip = "127.0.0.2"
port = 9916
bootstrap_ip = "127.0.0.1"
#bootstrap_ip = "192.168.0.1"
bootstrap_port = 9914
recv_length = 1024

node = Node(ip, port, False)

def create_server_socket():
	# create a socket that will be used by other nodes when they first connect
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
				[_, [_, succ]] = receive(client_socket)
				node.join([bootstrap_ip, bootstrap_port, client_socket], succ)
			# then check for other servers, they will try to connect to this one
			else:
				# the returned value is a pair (conn, address) where conn is a new socket object usable to send and
				# receive data on the connection, and address is the address bound to the socket on the other end of the connection.
				predecessor_socket, _ = node.get_sockets()[0].accept()
				# wait until info about predecessor and successor arrives
				[_, [pred, succ]] = receive(predecessor_socket)
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
			print('Reading error: '.format(str(e)))
			sys.exit()

def main_loop():
	while True:
		# iterate over all sockets, choose those that have been activated, set time interval to 0 for non-blocking
		read_sockets, _, exception_sockets = select.select(node.get_sockets(), [], node.get_sockets(), 0)

		# iterate over notified ones
		for notified_socket in read_sockets:
			if notified_socket == node.get_sockets()[0]:
				# the returned value is a pair (conn, address) where conn is a new socket object usable to send and
				# receive data on the connection, and address is the address bound to the socket on the other end of the connection.
				peer_socket, peer_address = node.get_sockets()[0].accept()
				# wait for info on port
				[[peer_id, _, code], pred] = receive(peer_socket)
				print("just received a new connection from", peer_id, "with info", pred)
				node.update_dht(pred[0], pred[1], peer_id, code, peer_socket)
			else:
				[[peer_id, count, code], info] = receive(notified_socket)
				# check for new successor
				if code == 0 or code == 2 or code == 3:
					[_, succ] = info
					node.update_dht(succ[0], succ[1], succ[2], code)
				#insert code
				elif code == 4:
					[key,value] = info
					node.insert(key,value)
				#delete code
				elif code == 5:
					[key] = info
					node.delete(key)
				#insert replica code
				elif code == 6:
					[key, value, peer_ip, peer_port, peer_id, currentk] = info
					node.replica_insert(key, value, peer_ip, peer_port, peer_id, currentk)

		# check for input, set time interval to 0 for non-blocking
		input = select.select([sys.stdin], [], [], 0)[0]
		if input:
			value = sys.stdin.readline().rstrip()
			if str(value) == "depart":
				node.depart()
				return
			elif str(value).lower().startswith("insert"):
				temporary = str(value)[6:].split(',')
				if (len(temporary) > 1):
					key = temporary[0].strip()
					some_value = temporary[1].strip()
					node.insert(key,some_value)

				# print("hashkey was",node.hash(key))
			elif str(value).lower().startswith("delete"):
				temporary = str(value)[6:]
				key = temporary.strip()
				some_value = temporary[1].strip()
				node.delete(key)
				# print("hashkey was",node.hash(key))
			print(f"You entered: {value}")

if __name__ == '__main__':
	# print(f"Arguments count: {len(sys.argv)}")
	# for i, arg in enumerate(sys.argv):
	# 	print(f"Argument {i:>6}: {arg}")
	create_server_socket()
	connect_to_dht()
	main_loop()
