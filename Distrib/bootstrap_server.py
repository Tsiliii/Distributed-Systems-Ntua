import socket
import select
import sys
import pickle
import errno
from backend import Node

#ip = "192.168.0.1"
ip = "127.0.0.1"
port = 9914
recv_length = 1024

node = Node(ip, port, True)
def create_server_socket():
	# create a socket that will be used by other nodes when they first connect
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_socket.bind((ip, port))
	# enable the server to accept connections
	server_socket.listen()

	# list of sockets for select.select()
	node.add_socket(server_socket)

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

		# iterate over notified sockets
		for notified_socket in read_sockets:
			node.set_counter()
			# if notified socket is a server socket - new connection, accept it
			if notified_socket == node.get_sockets()[0]:
				# the returned value is a pair (conn, address) where conn is a new socket object usable to send and
				# receive data on the connection, and address is the address bound to the socket on the other end of the connection.
				peer_socket, _ = notified_socket.accept()
				# receive port number
				[[peer_id, _, code], [peer_ip_address, peer_port]] = receive(peer_socket)
				print(f"node {peer_id} attempted to connect with {peer_ip_address} and {peer_port}")
				# if it is the new predecessor then peer_socket must be monitored
				if node.in_between_pred(peer_id) and code == 0 and node.get_predecessor() != None:
					node.add_socket(peer_socket)
				node.update_dht(peer_ip_address, peer_port, peer_id, code, peer_socket)
			else:
				[[peer_id, count, code], info] = receive(notified_socket)
				# check for new successor
				if code == 0 or code == 2 or code == 3:
					[_, succ] = info
					node.update_dht(succ[0], succ[1], succ[2], code)
				elif code == 1:
					[pred_ip, pred_port] = info
					node.update_dht(pred_ip, pred_port, peer_id, code=1, peer_socket=notified_socket)

		# check for input, set time interval to 0 for non-blocking
		input = select.select([sys.stdin], [], [], 0)[0]
		if input:
			value = sys.stdin.readline().rstrip()
			if str(value) == "depart":
				node.depart()
			elif str(value).lower().startswith("insert"):
				temporary = str(value)[6:].split(',')
				key = temporary[0].lstrip()
				value = temporary[1].lstrip()
				node.insert(key,value)
			print(f"You entered: {value}")

if __name__ == '__main__':
	create_server_socket()
	main_loop()
