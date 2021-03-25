import socket
import select
import sys
import pickle
import errno
from time import sleep
from backend import Node

#ip = "192.168.0.1"
ip = "127.0.0.1"
port = 9910
recv_length = 1024

node = Node(ip, port, True)

def create_server_socket(node):
	# create a socket that will be used by other nodes when they first connect
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# reuse adress for debugging only
	server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

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
			socket.close()
			continue
		except EOFError:
			continue
		# except Exception as e:
		# 	print("Some error occured, probably some node didn't depart correctly: ".format(str(e)))
		# 	print(socket.fileno())
		# 	print(str(e))
		# 	sys.exit()

def main_loop(node):
	while True:
		# iterate over all sockets, choose those that have been activated, set time interval to 0 for non-blocking
		read_sockets, _, exception_sockets = select.select(node.get_sockets(), [], node.get_sockets(), 0)

		# iterate over notified sockets
		for notified_socket in read_sockets:
			print("NOTIFIED",notified_socket)
			print("PRED",node.get_predecessor())
			print("SUCC",node.get_successor())
			print("SOCKS",node.get_sockets())
			node.set_counter()
			# if notified socket is a server socket - new connection, accept it
			if notified_socket == node.get_sockets()[0]:
				# print("GOT IN",notified_socket, node.get_sockets()[0],node.get_sockets())
				# the returned value is a pair (conn, address) where conn is a new socket object usable to send and
				# receive data on the connection, and address is the address bound to the socket on the other end of the connection.
				peer_socket, _ = notified_socket.accept()

				# receive port number
				[[peer_id, _, code], info] = receive(peer_socket)

				if code == 3:
					print("INFO",info)
					print("SOCKS2",node.get_sockets())
					[_, succ] = info
					node.update_dht(succ[0], succ[1], succ[2], code, peer_socket)
				else:
					[peer_ip_address, peer_port] = info
					print(f"node {peer_id} attempted to connect with {peer_ip_address} and {peer_port}")
					# if it is the new predecessor then peer_socket must be monitored
					if node.in_between_pred(peer_id) and code == 0 and node.get_predecessor() != None:
						node.add_socket(peer_socket)
						if node.get_successor() != node.get_predecessor():
							node.remove_socket(node.get_predecessor()[2])
						print("new socket, ",peer_socket)
					# print("COMPARE",peer_socket, node.get_sockets()[0])
					node.update_dht(peer_ip_address, peer_port, peer_id, code, peer_socket)
					# print(node.get_sockets())
				print()
			elif notified_socket not in node.get_sockets():
				continue
			else:
				# print(receive(notified_socket))
				[[peer_id, count, code], info] = receive(notified_socket)
				print(code, info)
				# check for new successor
				if code == 0 or code == 2 or code == 3:
					[_, succ] = info
					node.update_dht(succ[0], succ[1], succ[2], code)
				elif code == 1:
					[pred_ip, pred_port] = info
					node.update_dht(pred_ip, pred_port, peer_id, code = 1, peer_socket = notified_socket)
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
				#delete replica code
				elif code == 7:
					[key, peer_ip, peer_port, peer_id, currentk] = info
					node.replica_delete(key, peer_ip, peer_port, peer_id, currentk)
				#query code
				elif code == 8:
					[key, starting_node_ID, round_trip] = info
					node.query(key, starting_node_ID, made_a_round_trip = round_trip)
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
				print()


		# check for input, set time interval to 0 for non-blocking
		input = select.select([sys.stdin], [], [], 0)[0]
		if input:
			print()
			value = sys.stdin.readline().rstrip()
			if str(value) == "depart":
				# node.depart()
				print("This is the bootstrap node you probably shouldn't depart!!\nIf you are sure use exit or even Ctrl-C")
			elif str(value).lower().startswith("insert"):
				temporary = str(value).split(',')
				if (len(temporary) > 2):
					key = temporary[1].strip()
					some_value = temporary[2].strip()
					node.insert(key,some_value)
			elif str(value).lower().startswith("delete"):
				temporary = str(value).split(',')
				if (len(temporary) > 1):
					key = temporary[1].strip()
					node.delete(key)
			elif str(value).lower().startswith("query"):
				temporary = str(value).split(',')
				if (len(temporary) > 1):
					starting_node_ID = node.get_id()
					key = temporary[1].strip()
					node.query(key, starting_node_ID)
			elif str(value).lower().startswith("debug"):
				print(node.get_predecessor())
				print(node.get_successor())
				for sock in node.get_sockets():
					print(sock)
			elif str(value).lower().startswith("exit"):
				if (node.get_successor() == None):
					print('\033[1m' + "Hasta la vista, baby" + '\033[0m')
					print("""
█████████████████████████████████████
███████▀█████████████████████████████
██████░░█████████████████████████████
█████▀░▄█████████████████████████████
█████░░▀▀▀▀▀███▀▀█████▀▀███▀▀▀▀▀█████
█████░░▄██▄░░███░░███░░███░░███░░████
████░░█████░░███░▄███░░██░░▀▀▀░▄█████
████░░████▀░███░░███▀░███░░██████████
████░░█▀▀░▄████▄░▀▀▀░▄███▄░▀▀▀░▄█████
█████▄▄▄███████████░░██████▄▄████████
██████████████░▀█▀░▄█████████████████
███████████████▄▄▄███████████████████
█████████████████████████████████████

                      ______
                    <((((((((
                    /      . }
                    ;--..--._|}
( \                 '--/\--'  )
 \ \                | '-'  :'|
  \ \               . -==- .-|
   \ \               \.__.'   \--._
   [\ \          __.--|       //  _/'--._
   \ \ \       .'-._ ('-----'/ __/       |
    \ \ \     /   __>|      | '--.       |
     \ \ \   |   \   |     /    /       /
      \ '\ /     \  |     |  _/       /
       \  \       \ |     | /        /
        \  \       \       /
					""")
					return
				else:
					print("Some nodes are still connected, I can't shutdown")
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

if __name__ == '__main__':
	for i, arg in enumerate(sys.argv):
		if arg == '--k' and len(sys.argv) > i + 1 and sys.argv[i+1].isdigit():
			node.set_k(int(sys.argv[i+1]))
		elif arg == '--cons' and len(sys.argv) > i + 1:
			if sys.argv[i+1] in ['lazy','linearizability']:
				node.set_consistency(sys.argv[i+1])
	create_server_socket(node)
	main_loop(node)
