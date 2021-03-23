import socket
import pickle
from hashlib import sha1
from time import sleep

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
		self.counter = 0
		self.k = 1
		self.consistency = 'lazy' #or 'linearizability'
		self.replica_counter = {}

	def get_id(self, first4 = True):
		# if first4:
		# 	return int(str(self.id)[:4])
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

	def set_consistency(self, newconsistency):
		self.consistency = newconsistency

	def get_consistency(self):
		return self.consistency

	def set_k(self, newk):
		self.k = newk

	def get_k(self):
		return self.k

	def get_data_replica_counter(self,key):
		return self.replica_counter[key]

	def incr_data_replica_counter(self,key):
		self.replica_counter[key] += 1

	def get_data(self, key):
		if key == "*":
			return self.data
		return self.data[key]

	def check_if_in_data(self, key):
		return key in self.data

	def insert_data(self, key, value, currentk):
		self.data[key] = value
		self.replica_counter[key] = currentk

	def update_data(self, key, value):
		self.data[key] = value

	def delete_data(self, key):
		if self.check_if_in_data(key):
			del self.data[key]
			del self.replica_counter[key]

	def set_counter(self):
		self.counter += 1

	# assumes it will be used every time its value is requested
	def get_counter(self):
		self.counter += 1
		return self.counter

	def insert(self, key, value):
		#one node case
		if self.get_predecessor() == None:
			if self.check_if_in_data(key):
				print("I,",self.get_id(),"just updated key:",key,"with new value:",value,'and old value:',self.get_data(key))
				self.update_data(key,value)
				return
			else:
				print("I,",self.get_id(),"just inserted data",key,"with hash id",self.hash(key))
				self.insert_data(key,value,self.get_k())
				return

		pred = self.get_predecessor()[0]
		me = self.get_id()
		hashkey = self.hash(key)
		if ( (me >= hashkey and pred < hashkey) or (me < pred and (( me <= hashkey and pred < hashkey ) or (hashkey <= me and hashkey < pred)))):
			if self.check_if_in_data(key):
				print("I,",self.get_id(),"just updated key:",key,"with new value:",value,'and old value:',self.get_data(key))
				self.update_data(key,value)
				if self.get_k() != 1:
					msg = [[self.get_id(), self.get_counter(), 6],[key, value, self.get_ip_address(), self.get_port(), self.get_id(), self.get_k() - 1]]
					msg = pickle.dumps(msg, -1)
					self.get_successor()[2].send(msg)
				return
			else:
				print("I,",self.get_id(),"just inserted data",key,"with hash id",self.hash(key))
				self.insert_data(key,value,self.get_k())
				if self.get_k() != 1:
					msg = [[self.get_id(), self.get_counter(), 6],[key, value, self.get_ip_address(), self.get_port(), self.get_id(), self.get_k() - 1]]
					msg = pickle.dumps(msg, -1)
					# print(self.get_successor())
					self.get_successor()[2].send(msg)
				return
		else:
			print("Found insert for",self.hash(key),", passing it forward")
			msg = [[self.get_id(), self.get_counter(), 4],[key,value]]
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
			return

	def replica_insert(self,key,value,peer_ip,peer_port,peer_id,currentk):
		if currentk != 0 and self.get_id() != peer_id:
			if self.check_if_in_data(key):
				print("I,",self.get_id(),"just updated replica",currentk ,"for key:",key,"with new value:",value,'and old value:',self.get_data(key))
				self.update_data(key,value)
			else:
				print("I,",self.get_id(),"just inserted replica",currentk ,"for key:", key, "with hash id", self.hash(key))
				self.insert_data(key,value,currentk)
			if currentk != 1:
				msg = [[self.get_id(), self.get_counter(), 6],[key, value, peer_ip, peer_port, peer_id, currentk - 1]]
				msg = pickle.dumps(msg, -1)
				self.get_successor()[2].send(msg)
		elif self.get_id() == peer_id and currentk != 0:
			print("Finished the whole circle, network cardinality is smaller than k =",self.get_k())
		return

	def query(self, key, starting_node_ID, made_a_round_trip=False):
		if key == "*":
			tuples = self.get_data(key)
			if tuples:
				print("Key => Value pairs for node: ",self.get_id())
				for x in tuples.keys():
					print(x +" => " + tuples[x])
			else:
				print("Found request for query *, this node has no data")
			succ = self.get_successor()
			[ID, address, socket] = succ
			# if ID == starting_node_ID
			if int(str(ID)[:4]) == starting_node_ID:
				return
			if self.get_predecessor() == None:
				return
			msg = [[self.get_id(), self.get_counter(), 8],[key, starting_node_ID, False]]
			msg = pickle.dumps(msg, -1)
			succ[2].send(msg)
			return
		else:
			if self.consistency == "lazy":
				if self.check_if_in_data(key): #That means that the node is in the replication chain
					print(key + " => " + self.get_data(key))
					return
				else: #That means the node is outside the replication chain, we must find the first node that has it
					succ = self.get_successor()
					[ID, address, socket] = succ
					if self.get_id() == starting_node_ID:
						if made_a_round_trip:
							print("The key wasn't found!")
							return
						else:
							made_a_round_trip = True
					print("The key wasn't found on this node, passing the query forward")
					if self.get_predecessor() == None:
						return
					msg = [[self.get_id(), self.get_counter(), 8],[key, starting_node_ID, made_a_round_trip]]
					msg = pickle.dumps(msg, -1)
					succ[2].send(msg)
					return
			elif self.consistency == "linearizability":
				if not self.check_if_in_data(key):
					succ = self.get_successor()
					if self.get_id() == starting_node_ID:
						if made_a_round_trip:
							print("The key wasn't found!")
							return
						else:
							made_a_round_trip = True
					print("The key wasn't found on this node, passing the query forward")
					if self.get_predecessor() == None:
						return
					msg = [[self.get_id(), self.get_counter(), 8],[key, starting_node_ID, made_a_round_trip]]
					msg = pickle.dumps(msg, -1)
					succ[2].send(msg)
					return
				else:
					if self.get_data_replica_counter(key) != 1:
						succ = self.get_successor()
						print("The key was found on this node, but it's not the latest node, passing the query forward")
						if self.get_predecessor() == None:
							return
						msg = [[self.get_id(), self.get_counter(), 8],[key, starting_node_ID]]
						msg = pickle.dumps(msg, -1)
						succ[2].send(msg)
						return
					else:
						print(key + " => " + self.get_data(key))
						return

	def delete(self, key):
			#one node case
			if self.get_predecessor() == None:
				if self.check_if_in_data(key):
					print("I,",self.get_id(),"just updated key:",key,"with new value:",value,'and old value:',self.get_data(key))
					self.update_data(key,value)
					return
				else:
					print("I,",self.get_id(),"just inserted data",key,"with hash id",self.hash(key))
					self.insert_data(key,value,self.get_k())
					return

			pred = self.get_predecessor()[0]
			me = self.get_id()
			hashkey = self.hash(key)

			#one node case
			if me == pred:
				if self.check_if_in_data(key):
					print("I,",self.get_id(),"just deleted key:",key,"with value:",self.get_data(key))
					self.delete_data(key)
					return
				else:
					print("I,",self.get_id(),"tried to delete missing data with key",key)
					return

			# print(pred,hashkey,me)
			if ( (me >= hashkey and pred < hashkey) or (me < pred and (( me <= hashkey and pred < hashkey ) or (hashkey <= me and hashkey < pred)))):
				if self.check_if_in_data(key):
					print("I,",self.get_id(),"just deleted key:",key,"with value:",self.get_data(key))
					self.delete_data(key)
					if self.get_k() != 1:
						msg = [[self.get_id(), self.get_counter(), 7],[key, self.get_ip_address(), self.get_port(), self.get_id(), self.get_k() - 1]]
						msg = pickle.dumps(msg, -1)
						self.get_successor()[2].send(msg)
				else:
					print("I,",self.get_id(),"tried to delete missing data with key",key)
					return
			else:
				print("Found delete for",self.hash(key),", passing it forward")
				msg = [[self.get_id(), self.get_counter(), 5],[key]]
				msg = pickle.dumps(msg, -1)
				self.get_successor()[2].send(msg)
				return

	def replica_delete(self,key,peer_ip,peer_port,peer_id,currentk):
		if currentk != 0 and self.get_id() != peer_id:
			if self.check_if_in_data(key):
				print("I,",self.get_id(),"just deleted replica",currentk ,"with key:",key,'and value:',self.get_data(key))
				self.delete_data(key)
			else:
				print("I,",self.get_id(),"tried to delete missing data with key",key,".Something wrong must have happened")
			if currentk != 1:
				msg = [[self.get_id(), self.get_counter(), 7],[key, peer_ip, peer_port, peer_id, currentk - 1]]
				msg = pickle.dumps(msg, -1)
				self.get_successor()[2].send(msg)
		elif self.get_id() == peer_id and currentk != 0:
			print("Finished the whole circle, network cardinality is smaller than k =",self.get_k())
		return

	# use sha1 to compute id
	def compute_id(self, ip_address, port):
		# return port%100
		# assumes ip_address is a string and port is an int
		digest = sha1((ip_address+':'+str(port)).encode('ascii')).hexdigest()
		# return int(digest, 16)
		return int(str(int(digest, 16))[:4])

	def hash(self, key):
		digest = sha1((str(key)).encode('ascii')).hexdigest()
		# return int(digest, 16)
		return int(str(int(digest, 16))[:4])

	def add_socket(self, socket):
		if socket not in self.socket_list:
			self.socket_list.append(socket)

	def remove_socket(self, socket):
		self.socket_list.remove(socket)

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
			msg = [[self.get_id(), self.get_counter(), 0], [[self.ip_address, self.port, self.get_id()], [self.ip_address, self.port, self.get_id()], [self.get_k(), self.get_consistency()]]]
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
			if code == 2 and self.is_bootstrap and self.get_id() == peer_id: # bootstrap is alone
				print("I am all alone in this world yet again")
				for i,sock in enumerate(self.get_sockets()):
					if i != 0:
						self.remove_socket(self.get_sockets()[i])
				self.set_predecessor(None)
				self.set_successor(None)
				return
			if peer_id != self.get_predecessor()[0]:   # checking if pred and succ is the same node
				if peer_socket == None:
					peer_socket = self.create_socket(peer_ip_address, peer_port)
				self.add_socket(peer_socket)
				self.set_successor([peer_id, [peer_ip_address, peer_port], peer_socket])
			else:
				self.set_successor([peer_id, [peer_ip_address, peer_port], self.get_predecessor()[2]])
			if code == 0:
				msg = [[self.get_id(), self.get_counter(), 0], [[self.ip_address, self.port, self.get_id()], [former_successor[1][0], former_successor[1][1], former_successor[0]] , [self.get_k(), self.get_consistency()]]]
			elif code == 1:
				msg = [[self.get_id(), self.get_counter(), 1], [self.get_ip_address(), self.get_port()]]
			elif code == 2:
				msg = [[self.get_id(), self.get_counter(), 3], [[self.ip_address, self.port, self.get_id()], [self.ip_address, self.port, self.get_id()]]]
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
			print("found_successor: ", peer_id)

		# if it is not the successor of the current node
		else:
			msg = [[self.get_id(), self.get_counter(), 0], [[self.ip_address, self.port, self.get_id()], [peer_ip_address, peer_port, peer_id]]]
			msg = pickle.dumps(msg, -1)
			print("node:", peer_id)
			self.get_successor()[2].send(msg)

	def join(self, pred, succ, bootstrap_socket=None):
		self.add_socket(pred[2])
		# check if this is the second node to be connected
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
		# Send the data that this node has to the successor:
		node_data = self.get_data("*")
		if node_data:
			if self.get_k() != 1:
				sent_data = {}
				sent_key = set()
				for key, value in node_data.items():
					if self.get_data_replica_counter(key) == 1:
						sent_data[key] = value
					else:
						sent_key.add(key)
				departing_node_id = self.get_id()
				msg = [[self.get_id(), self.get_counter(), 9], [sent_data, sent_key, departing_node_id]]
				msg = pickle.dumps(msg,-1)
				self.get_successor()[2].send(msg)
				sleep(1)
			else:
				sent_data = node_data
				sent_key = set()
				departing_node_id = self.get_id()
				msg = [[self.get_id(), self.get_counter(), 9], [sent_data, sent_key, departing_node_id]]
				msg = pickle.dumps(msg,-1)
				self.get_successor()[2].send(msg)
				sleep(1)

		# network adjustments
		successor = self.get_successor()
		msg = [[self.get_id(), self.get_counter(), 2], [[self.ip_address, self.port, self.get_id()], [successor[1][0], successor[1][1], successor[0]]]]
		msg = pickle.dumps(msg, -1)

		# case
		if self.get_successor()[0] == self.get_predecessor()[0]:
			self.get_predecessor()[2].send(msg)
			print("Only node remaining is Bootstrap")
			self.get_successor()[2].shutdown(socket.SHUT_RDWR)
			self.get_successor()[2].close()
			return

		self.get_predecessor()[2].send(msg)
		self.get_predecessor()[2].shutdown(socket.SHUT_RDWR)
		self.get_predecessor()[2].close()
		sleep(.1)
		self.get_successor()[2].shutdown(socket.SHUT_RDWR)
		self.get_successor()[2].close()

		for i,sock in enumerate(self.get_sockets()):
			if i != 0:
				self.remove_socket(self.get_sockets()[i])
		print(f"sending message to {self.get_predecessor()[2]}")

	def update_data_on_join():
		pass

	def update_data_on_depart(self, sent_data, sent_key, departing_node_id):
		if self.get_k() != 1:
			if sent_data or sent_key:
				for key,value in sent_data.items():
					self.insert_data(key,value,1)
				sent_data = {}
				new_sent_key = {}
				for key in sent_key:
					current_counter = self.get_data_replica_counter(key)
					if current_counter == 1:
						sent_data[key] = self.get_data(key)
					else:
						new_sent_key.add(key)
					self.incr_data_replica_counter(key)

			if departing_node_id != self.get_successor()[2] and (sent_data or new_sent_key):
				msg = [[self.get_id(), self.get_counter(), 9], [sent_data, new_sent_key, departing_node_id]]
				msg = pickle.dumps(msg,-1)
				self.get_successor()[2].send(msg)
			return
		else:
			for key, value in sent_data.items():
				self.insert_data(key,value,1)
			return

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
