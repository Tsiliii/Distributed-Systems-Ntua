import socket
import pickle
from hashlib import sha1

class Node():
	def __init__(self, ip_address, port, is_bootstrap):
		self.ip_address = ip_address
		self.port = port
		self.id = self.compute_id(ip_address, port)
		print("my id:", self.id)
		self.is_bootstrap = is_bootstrap
		self.data = {}
		self.successor = None	  # in the form [id, address, socket]
		self.predecessor = None		# in the form [id, address, socket]
		self.socket_list = []
		self.sockets_to_be_closed = []
		self.counter = 0
		self.k = 1

	def get_id(self):
		return self.id

	def get_ip_address(self):
		return self.ip_address

	def get_port(self):
		return self.port

	def set_predecessor(self, predecessor):
		self.predecessor = predecessor

	def get_predecessor(self):
		return self.predecessor

	def set_successor(self, successor):
		self.successor = successor

	def get_successor(self):
		return self.successor

	def get_data(self, key):
		return self.data[key]

	def check_if_in_data(self, key):
		return key in self.data

	def insert_data(self, key, value):
		self.data[key] = value

	def update_data(self, key, value):
		self.data[key] = value

	def set_counter(self):
		self.counter += 1

	# assumes it will be used every time its value is requested
	def get_counter(self):
		self.counter += 1
		return self.counter

	def insert(self, key, value):
		if self.k == 1:
			pred = self.get_predecessor()[0]
			me = self.get_id()
			hashkey = self.hash(key)
			print(pred,hashkey,me)
			if ( me >= hashkey and pred < hashkey or (me < pred and ( me <= hashkey and pred < hashkey ) or (hashkey <= me and hashkey < pred) ) ):
				if self.check_if_in_data(key):
					print("I,",self.get_id(),"just updated key:",key,"with new value:",value,'and old value:',self.get_data(key))
					self.update_data(key,value)
					# maybe sth like
					# return previous data?
				else:
					print("I,",self.get_id(),"just inserted data",key,"with hash id",self.hash(key))
					self.insert_data(key,value)
					# return Ok?
			else:
				print("Found insert for",self.hash(key),", passing it forward")
				msg = [["", "", 4],[key,value]]
				msg = pickle.dumps(msg, -1)
				print(self.get_successor())
				self.get_successor()[2].send(msg)
				# demand insert in successor

	def query(self, key, k):
		return

	def delete(self, key):
		return

	# use sha1 to compute id
	def compute_id(self, ip_address, port):
		# return port%100
		# assumes ip_address is a string and port is an int
		digest = sha1((ip_address+':'+str(port)).encode('ascii')).hexdigest()
		return int(digest, 16)

	def hash(self, key):
		digest = sha1((str(key)).encode('ascii')).hexdigest()
		return int(digest, 16)

	def add_socket(self, socket):
		if socket not in self.socket_list:
			self.socket_list.append(socket)

	def remove_socket(self, socket):
		self.socket_list.remove(socket)
		self.sockets_to_be_closed.append(socket)

	def close_sockets(self):
		for socket in sockets_to_be_closed:
			socket.close()

	def for_future_closing(self, socket):
		sockets_to_be_closed.append(socket)

	def get_sockets(self):
		return self.socket_list

	def after_successor(self, x):
		successor = self.get_successor()
		if successor == None:
			return True
		successor_id = successor[0]
		id = self.get_id()
		return (x > successor_id and x < id) or (x < id and id < successor_id) or (id < successor_id and x > successor_id)

	def in_between_pred(self, x):
		predecessor = self.get_predecessor()
		if predecessor == None:
			return True
		predecessor_id = predecessor[0]
		id = self.get_id()
		#print("in_between_pred", x < id and id < predecessor_id, id < predecessor_id and predecessor_id < x, predecessor_id < x and x < id)
		return (x < id and id < predecessor_id) or (id < predecessor_id and predecessor_id < x) or (predecessor_id < x and x < id)

	def in_between_succ(self, x):
		successor = self.get_successor()
		if successor == None:
			return True
		successor_id = successor[0]
		id = self.get_id()
		#print("in_between_succ", x < successor_id and successor_id < id, successor_id < id and id < x, id < x and x < successor_id)
		return (x < successor_id and successor_id < id) or (successor_id < id and id < x) or (id < x and x < successor_id)

	def update_dht(self, peer_ip_address, peer_port, peer_id, code, peer_socket=None):
		# code = 1 only in the case when this node is the successor
		# second node entered
		if self.is_bootstrap and self.get_successor() == None:
			self.set_successor([peer_id, [peer_ip_address, peer_port], peer_socket])
			self.set_predecessor([peer_id, [peer_ip_address, peer_port], peer_socket])
			self.add_socket(peer_socket)
			# let second node know what's up
			print("2nd node entered: ", peer_id)
			msg = [[self.get_id(), self.get_counter(), 0], [[self.ip_address, self.port, self.id], [self.ip_address, self.port, self.id]]]
			msg = pickle.dumps(msg, -1)
			peer_socket.send(msg)
		# if we have just been informed about a new predecessor
		elif (self.in_between_pred(peer_id) and code == 1) or code == 3:
			former_predecessor = self.get_predecessor()
			# remove socket only node to be added is not the third one
			if former_predecessor[0] != self.get_successor()[0]:
				self.remove_socket(former_predecessor[2])
			if peer_id != self.get_successor()[0]:
				self.add_socket(peer_socket)
				self.set_predecessor([peer_id, [peer_ip_address, peer_port], peer_socket])
			else:
				self.set_predecessor([peer_id, [peer_ip_address, peer_port], self.get_successor()[2]])
			print("informed of a predecessor: ", peer_id)
		# if it is the successor of the current node
		elif self.in_between_succ(peer_id) or code == 2:
			former_successor = self.get_successor()
			# remove socket only node to be added is not the third one
			if former_successor[0] != self.get_predecessor()[0]:
				self.remove_socket(former_successor[2])
			if peer_id != self.get_predecessor()[0]:   # checking if pred and succ is the same node
				if peer_socket == None:
					peer_socket = self.create_socket(peer_ip_address, peer_port)
				self.add_socket(peer_socket)
				self.set_successor([peer_id, [peer_ip_address, peer_port], peer_socket])
			else:
				self.set_successor([peer_id, [peer_ip_address, peer_port], self.get_predecessor()[2]])
			if code == 0:
				msg = [[self.get_id(), self.get_counter(), 0], [[self.ip_address, self.port, self.id], [former_successor[1][0], former_successor[1][1], former_successor[0]]]]
			elif code == 1:
				msg = [[self.get_id(), self.get_counter(), 1], [self.get_ip_address(), self.get_port()]]
			elif code == 2:
				msg = [[self.get_id(), self.get_counter(), 3], [[self.ip_address, self.port, self.id], [self.ip_address, self.port, self.id]]]
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
			print("found_successor: ", peer_id)
		# if it is not the successor of the current node
		else:
			msg = [[self.get_id(), self.get_counter(), 0], [[self.ip_address, self.port, self.id], [peer_ip_address, peer_port, peer_id]]]
			msg = pickle.dumps(msg, -1)
			print("node:", peer_id)
			self.get_successor()[2].send(msg)

	def join(self, pred, succ, bootstrap_socket=None):
		self.add_socket(pred[2])
		# check if this is the socond node to be connected
		if self.compute_id(pred[0], pred[1]) == succ[2]:
			self.set_predecessor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])
			self.set_successor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])
		elif bootstrap_socket == None:
			self.set_predecessor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])
			new_successor_socket = self.create_socket(succ[0], succ[1])
			self.set_successor([succ[2], [succ[0], succ[1]], new_successor_socket])
			self.add_socket(new_successor_socket)
			msg = [[self.get_id(), self.get_counter(), 1], [self.get_ip_address(), self.get_port()]]
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
		else:
			self.set_predecessor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])
			self.set_successor([succ[2], [succ[0], succ[1]], bootstrap_socket])
			self.add_socket(bootstrap_socket)
			msg = [[self.get_id(), self.get_counter(), 1], [self.get_ip_address(), self.get_port()]]
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
		print(f"Server {self.get_id()} with address {self.ip_address} and port {self.port} just joined")

	def depart(self):
		successor = self.get_successor()
		msg = [[self.get_id(), self.get_counter(), 2], [[self.ip_address, self.port, self.id], [successor[1][0], successor[1][1], successor[0]]]]
		msg = pickle.dumps(msg, -1)
		self.get_predecessor()[2].send(msg)
		for socket in self.get_sockets():
			self.remove_socket(socket)
		print(f"sending message to {self.get_predecessor()[2]}")

	def create_socket(self, ip_address, port):
		print(f"creating socket for address {ip_address} and port {port}")
		client_socket = server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		client_socket.connect((ip_address, port))
		self.set_counter()
		# set connection to non-blocking state, so .recv() call won't block, just return some exception we'll handle
		# if blocking was set to True that would mean wait until something is received
		client_socket.setblocking(False)
		# inform successor on port
		return client_socket
