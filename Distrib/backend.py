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

	def decr_data_replica_counter(self,key):
		self.replica_counter[key] -= 1

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
					print(x +" => " + tuples[x], self.get_data_replica_counter(x))
			else:
				print("Found request for query *, this node has no data")
			if self.get_predecessor() == None:
				return
			succ = self.get_successor()
			[ID, address, socket] = succ
			# if ID == starting_node_ID
			if int(str(ID)[:4]) == starting_node_ID:
				return
			msg = [[self.get_id(), self.get_counter(), 8],[key, starting_node_ID, False]]
			msg = pickle.dumps(msg, -1)
			succ[2].send(msg)
			return
		else:
			if self.consistency == "lazy":
				if self.check_if_in_data(key): #That means that the node is in the replication chain
					print(key + " => " + self.get_data(key), self.get_data_replica_counter(key))
					return
				else: #That means the node is outside the replication chain, we must find the first node that has it
					if self.get_predecessor() == None:
						return
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
					if self.get_predecessor() == None:
						return
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
						print(key + " => " + self.get_data(key), self.get_data_replica_counter(key))
						return

	def delete(self, key):
			#one node case
			if self.get_predecessor() == None:
				if self.check_if_in_data(key):
					print("I,",self.get_id(),"just deleted key:",key,"with value:",self.get_data(key))
					self.delete_data(key)
					return
				else:
					print("I,",self.get_id(),"tried to delete missing data with key",key)
					return

			# print(pred,hashkey,me)
			me, hashkey, pred = self.get_id(), self.hash(key), self.get_predecessor()[0]
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
		if socket in self.socket_list:
			self.socket_list.remove(socket)
		else:
			print("Failed to remove",socket)

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
		# code = 0 until new node finds its pred
		# code = 1 sent to new node successor (messaged from new node)
		# code = 2 departed node pred
		# code = 3 departed node succ (messaged from departed node pred)

		print(peer_ip_address, peer_port, peer_id, code)
		# second node entered
		if self.is_bootstrap and self.get_successor() == None:
			self.set_successor([peer_id, [peer_ip_address, peer_port], peer_socket])
			print("1")
			self.set_predecessor([peer_id, [peer_ip_address, peer_port], peer_socket])
			self.add_socket(peer_socket)
			# let second node know what's up
			print("2nd node entered: ", peer_id)
			msg = [[self.get_id(), self.get_counter(), 0], [[self.get_ip_address(), self.get_port(), self.get_id()], [self.get_ip_address(), self.get_port(), self.get_id()], [self.get_k(), self.get_consistency()]]]
			msg = pickle.dumps(msg, -1)
			peer_socket.send(msg)
			print("Sent 1")

		# second node leaves
		elif self.is_bootstrap and code == 2 and self.get_successor()[0] == self.get_predecessor()[0]:
			self.set_successor(None)
			self.set_predecessor(None)
			if len(self.get_sockets()) > 1:
				to_close = self.get_sockets()[1]
				self.remove_socket(to_close)
				to_close.shutdown(socket.SHUT_RDWR)
				to_close.close()
			return

		# if we have just been informed about a new predecessor
		elif (self.in_between_pred(peer_id) and code == 1):
			former_predecessor = self.get_predecessor()

			# remove socket only node to be added is not the third one
			if former_predecessor[0] != self.get_successor()[0]:
				self.remove_socket(former_predecessor[2])
				former_predecessor[2].close()
				print("1,",self.get_id(), "closed", former_predecessor)
			self.add_socket(peer_socket)
			self.set_predecessor([peer_id, [peer_ip_address, peer_port], peer_socket])
			print("informed of a predecessor: ", peer_id)
			return

		elif code == 3:
			former_predecessor = self.get_predecessor()
			self.remove_socket(former_predecessor[2])
			former_predecessor[2].close()
			print("2,",self.get_id(), "closed", former_predecessor)
			print(peer_id,peer_socket)
			if self.get_successor()[0] != peer_id:
				self.add_socket(peer_socket)
			self.set_predecessor([peer_id, [peer_ip_address, peer_port], peer_socket])
			print("informed of a predecessor: ", peer_id)
			return

		# if it is the successor of the current node
		elif self.in_between_succ(peer_id) and code == 0:
			# create the new socket
			peer_socket = self.create_socket(peer_ip_address, peer_port)

			# close connection to successor and remove successor info if he is not predecessor aswell
			former_successor = self.get_successor()
			if self.get_successor()[0] != self.get_predecessor()[0]:   # checking if pred and succ is the same node
				self.remove_socket(former_successor[2])
				former_successor[2].shutdown(socket.SHUT_RDWR)
				former_successor[2].close()
				print("3,",self.get_id(), "closed", former_successor)

			# set the new successor
			self.add_socket(peer_socket)
			self.set_successor([peer_id, [peer_ip_address, peer_port], peer_socket])

			# print(self.get_successor())
			# print(self.get_predecessor())
			# print(self.get_sockets())
			msg = [[self.get_id(), self.get_counter(), 0], [[self.get_ip_address(), self.get_port(), self.get_id()], [former_successor[1][0], former_successor[1][1], former_successor[0]] , [self.get_k(), self.get_consistency()]]]
			print("Sending to my succ 0",self.get_successor(),msg)
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
			print("Sent 2")
			print("found_successor: ", peer_id)

		elif code == 2:

			# 3 node case (nasty)
			if peer_id == self.get_predecessor()[0] and self.get_successor()[0] != self.get_predecessor()[0]:
				former_successor = self.get_successor()

				# remove successor
				self.remove_socket(former_successor[2])
				former_successor[2].shutdown(socket.SHUT_RDWR)
				former_successor[2].close()

				#set new successor
				self.set_successor([peer_id, [peer_ip_address, peer_port], self.get_predecessor()[2]])

				# message new successor
				msg = [[self.get_id(), self.get_counter(), 3], [[self.get_ip_address(), self.get_port(), self.get_id()], [self.get_ip_address(), self.get_port(), self.get_id()]]]
				print("Sending to my successor 3",self.get_successor(),msg)
				msg = pickle.dumps(msg, -1)
				self.get_successor()[2].send(msg)
				print("found_successor: ", peer_id)
				return

			# close connection to successor and remove successor info if he is not predecessor aswell
			former_successor = self.get_successor()
			peer_socket = self.create_socket(peer_ip_address, peer_port)
			if self.get_successor()[0] != self.get_predecessor()[0]:   # checking if pred and succ is the same node
				self.remove_socket(former_successor[2])
				former_successor[2].shutdown(socket.SHUT_RDWR)
				former_successor[2].close()
				print("3,",self.get_id(), "closed", former_successor)
				self.add_socket(peer_socket)
			self.set_successor([peer_id,[peer_ip_address, peer_port], peer_socket])

			msg = [[self.get_id(), self.get_counter(), 3], [[self.get_ip_address(), self.get_port(), self.get_id()], [self.get_ip_address(), self.get_port(), self.get_id()]]]
			print("Sending to my successor 3",self.get_successor(),msg)
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
			print("found_successor: ", peer_id)

		# if it is not the successor of the current node
		else:
			msg = [[self.get_id(), self.get_counter(), 0], [[self.get_ip_address(), self.get_port(), self.get_id()], [peer_ip_address, peer_port, peer_id]]]
			msg = pickle.dumps(msg, -1)
			print("Found info about node:", peer_id)
			self.get_successor()[2].send(msg)

	def join(self, pred, succ, bootstrap_socket=None):
		self.add_socket(pred[2])
		# check if this is the second node to be connected
		if self.compute_id(pred[0], pred[1]) == succ[2]:
			print("5")
			self.set_predecessor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])
			self.set_successor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])

		elif bootstrap_socket == None:
			print("6")
			self.set_predecessor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])
			new_successor_socket = self.create_socket(succ[0], succ[1])
			self.set_successor([succ[2], [succ[0], succ[1]], new_successor_socket])
			self.add_socket(new_successor_socket)
			msg = [[self.get_id(), self.get_counter(), 1], [self.get_ip_address(), self.get_port()]]
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)

		else:
			print("7")
			self.set_predecessor([self.compute_id(pred[0], pred[1]), [pred[0], pred[1]], pred[2]])
			self.set_successor([succ[2], [succ[0], succ[1]], bootstrap_socket])
			self.add_socket(bootstrap_socket)
			msg = [[self.get_id(), self.get_counter(), 1], [self.get_ip_address(), self.get_port()]]
			msg = pickle.dumps(msg, -1)
			self.get_successor()[2].send(msg)
		print(f"Server {self.get_id()} with address {self.get_ip_address()} and port {self.get_port()} just joined")

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
		msg = [[self.get_id(), self.get_counter(), 2], [[self.get_ip_address(), self.get_port(), self.get_id()], [successor[1][0], successor[1][1], successor[0]]]]
		# print(msg)
		msg = pickle.dumps(msg, -1)

		sleep(1)
		# 2 nodes case where succ == pred == bootstrap
		if self.get_successor()[0] == self.get_predecessor()[0]:
			self.get_predecessor()[2].send(msg)
			print("Only node remaining is Bootstrap")
			sleep(.1)
			self.get_successor()[2].shutdown(socket.SHUT_RDWR)
			self.get_successor()[2].close()
			print("4,",self.get_id(), "closed", self.get_successor())
			return

		self.get_predecessor()[2].send(msg)
		print(f"sending message to {self.get_predecessor()[2]}")
		sleep(.1)
		self.get_predecessor()[2].shutdown(socket.SHUT_RDWR)
		self.get_predecessor()[2].close()
		print("5,",self.get_id(), "closed", self.get_predecessor())
		self.remove_socket(self.get_predecessor()[2])
		sleep(.1)
		self.get_successor()[2].shutdown(socket.SHUT_RDWR)
		self.get_successor()[2].close()
		print("6,",self.get_id(), "closed", self.get_successor())
		self.remove_socket(self.get_successor()[2])

		while (self.get_sockets()):
			self.get_sockets()[0].shutdown(socket.SHUT_RDWR)
			self.get_sockets()[0].close()
			self.remove_socket(self.get_sockets()[0])
			sleep(.1)
		return

	def update_data_on_join(self, new_node_ID, data_to_be_updated, counters_to_be_updated, message_sender_ID):

		# Special case: The node that just entered is only the 2nd node to be entered in the DHT:
		if self.get_successor()[0] == new_node_ID and self.get_predecessor()[0] == new_node_ID:
			my_data = self.get_data("*")
			# i am bootstrap and i check whether there is data to pass on
			if my_data:
				data_to_be_updated = {}
				counters_to_be_updated = {}
				for key, value in my_data.items():
					# item belongs to new node
					me, hashkey, pred = self.get_id(), self.hash(key), new_node_ID
					if (me >= hashkey and pred < hashkey) or (me < pred and (( me <= hashkey and pred < hashkey ) or (hashkey <= me and hashkey < pred))):
						if self.get_k() != 1:
							data_to_be_updated[key] = value
							counters_to_be_updated[key] = self.get_k() - 1
							print("I am keeping ownership of (", key,",",value,")")
					# item belongs to bootstrap
					else:
						data_to_be_updated[key] = value
						counters_to_be_updated[key] = self.get_k()
						print("I am giving ownership of (", key,",",value,",",counters_to_be_updated[key],"), to the new node")
						if self.get_k() == 1:
							self.delete_data(key)
						else:
							self.decr_data_replica_counter(key)
				# inform new node of his new data
				msg = [[self.get_id(), self.get_counter(), 10], [new_node_ID, data_to_be_updated, counters_to_be_updated, self.get_id()]]
				msg = pickle.dumps(msg,-1)
				self.get_successor()[2].send(msg)
				return
			else:
				return

		# Special case continue:
		if self.get_successor()[0] == self.get_predecessor()[0]:
			# just place everything with the correct replica counter key
			for key, value in data_to_be_updated.items():
				print("Inserted:  (", key,",",value,",",counters_to_be_updated[key],")")
				self.insert_data(key,value,counters_to_be_updated[key])
			return

		# if the node's ID is the same as the new node's ID, means data must be written (except for the items that belong to the new node).
		if self.get_id() == new_node_ID:
			# if the successor of the new node is sending the message, that means that the
			# message contains the data that the new node must contain, taken from the
			# node after this one.
			if message_sender_ID == self.get_successor()[0] and message_sender_ID != self.get_predecessor()[0]:
				for key, value in data_to_be_updated.items():
					if counters_to_be_updated[key] != 0:
						self.insert_data(key,value,counters_to_be_updated[key])
						print("Inserted:  (", key,",",value,",",counters_to_be_updated[key],")")

			# if the message is being sent from the predecessor, that means that the message
			# contains data that should be replicated more (i.e. the DHT does not have enough
			# nodes to cover the k parameter thus far)
			elif message_sender_ID == self.get_predecessor()[0] and message_sender_ID != self.get_successor()[0]:
				for key, value in data_to_be_updated.items():
					if counters_to_be_updated[key] == 0 or (self.check_if_in_data(key)):
						continue
					self.insert_data(key,value,counters_to_be_updated[key])
					print("Inserted:  (", key,",",value,",",counters_to_be_updated[key],")")

			print("I have just joined the DHT, and have inserted the data I was supposed to!")
			# print()
			return

		# if data and counters sets are empty, that means the message was sent from the node that just joined:
		if (not data_to_be_updated) and (not counters_to_be_updated) and self.get_predecessor()[0] == new_node_ID:
			node_after_new_node_data 	 = self.get_data("*")
			node_after_new_node_counters = self.replica_counter
			data_to_be_shifted_left = {}
			counters_to_be_shifted_left = {}
			# for each key, there are 3 possibilities:
			# 1) the node that just joined must be the owner of the data
			# 2) the node that just joined must have a replica of the data
			# 3) the node that just joined must not be the owner of the data and not have a replica of the data.
			for key, value in node_after_new_node_data.items():
				print("(",key,",", value,",",self.replica_counter[key],")")
				# Case (2)
				if node_after_new_node_counters[key] != self.get_k():
					data_to_be_updated[key] 	= node_after_new_node_data[key]
					counters_to_be_updated[key] = node_after_new_node_counters[key]
					# now that the new node takes a replica of the data, this action must be passed on, so that
					# the nodes after "shift" left-wise the data
					data_to_be_shifted_left[key] = value
					# reduce the replica counter of the data for this node (same must be done for next nodes)
					self.replica_counter[key] -= 1
					counters_to_be_shifted_left[key] = self.replica_counter[key]

					print("I am giving a replica of (", key,",",value,"), to the new node")

				# Case (1) if the new node according to the hashing function must own the data,
				# or Case (3) if according to the hashing function the data belong to the node
				# after the node that just joined.
				else:
					hashkey = self.hash(key)
					node_after_new_node_ID = self.get_id()
					# if it belongs to thhe node after the new node, then do nothing. (Case 3)
					if ( (node_after_new_node_ID >= hashkey and new_node_ID < hashkey) or \
						 (node_after_new_node_ID < new_node_ID and \
						 	(( node_after_new_node_ID <= hashkey and new_node_ID < hashkey ) or \
						 	(hashkey <= node_after_new_node_ID and hashkey < new_node_ID)))):
						print("I am keeping ownership of (", key,",",value,")")
						continue
					# if it belongs to the new node, pass it back. (Case 1)
					else:
						data_to_be_updated[key] 	= node_after_new_node_data[key]
						counters_to_be_updated[key] = node_after_new_node_counters[key]
						# reduce the replica counter of the data for this node (same must be done for next nodes)
						print("I am giving ownership of (", key,",",value,",",counters_to_be_updated[key],"), to the new node")

						# now that the new node takes a replica of the data, this action must be passed on, so that
						# the nodes after "shift" left-wise the data (even if its replica_counter == 0, must be on)
						self.replica_counter[key] -= 1
						counters_to_be_shifted_left[key] = self.replica_counter[key]
						data_to_be_shifted_left[key] 	 = value


			# if the replica counter of a specific datum reaches 0, means the key should be deleted:
			# should be outside the for loop, because it deletes items from the dict, python does NOT like that.
			keys_to_be_deleted = []
			for key, value in node_after_new_node_data.items():
				if self.replica_counter[key]  == 0:
					keys_to_be_deleted.append(key)
			for key in keys_to_be_deleted:
				if self.replica_counter[key] == 0:
					del self.data[key]
					del self.replica_counter[key]
				del data_to_be_shifted_left[key]
				del counters_to_be_shifted_left[key]
				continue

			# Now, the node after the node that just joined must send messages. First to the node that just joined,
			# sending all the data that the new node should have, and then to its successor, so that the action of
			# the shifting must be passed on, meaning that a new node has just joined, and the replicas are being
			# shifted left.
			msg = [[self.get_id(), self.get_counter(), 10], [new_node_ID, data_to_be_updated, counters_to_be_updated, self.get_id()]]
			msg = pickle.dumps(msg,-1)
			self.get_predecessor()[2].send(msg)

			# send message now to successors:
			msg = [[self.get_id(), self.get_counter(), 10], [new_node_ID, data_to_be_shifted_left, counters_to_be_shifted_left, self.get_id()]]
			msg = pickle.dumps(msg,-1)
			self.get_successor()[2].send(msg)
			# print()

			return

		# Finally, if the node is a distant successor of the node that just joined:
		if self.get_id() != new_node_ID and self.get_predecessor()[0] != new_node_ID:
			# shift left anything that has to be shifted left:
			for key, value in data_to_be_updated.items():
					if self.get_data_replica_counter(key) != self.get_k():
							self.decr_data_replica_counter(key)
							# self.insert_data(key,value,counters_to_be_updated[key])
							counters_to_be_updated[key] -= 1
							print("Passing on (", key,",",value,",",counters_to_be_updated[key],")")

			# if the replica counter of a specific datum reaches 0, means the key should be deleted:
			# should be outside the for loop, because it deletes items from the dict, python does NOT like that.

			keys_to_be_deleted = []
			for key, value in data_to_be_updated.items():
					if self.replica_counter[key]  == 0 or self.replica_counter[key] == self.get_k():
							keys_to_be_deleted.append(key)
			for key in keys_to_be_deleted:
					if self.replica_counter[key] == 0:
							del self.data[key]
							del self.replica_counter[key]
					del data_to_be_updated[key]
					del counters_to_be_updated[key]
					continue

			# If the successor node is the new node, that means that we have had a full circle.
			# That means that we also have to check if the full circle is less than the k
			# parameter, and possibly have to pass on replicas that wouldn't be passed on
			# otherwise.
			if self.get_successor()[0] == new_node_ID:
				for key, value in self.data.items():
					if key not in data_to_be_updated and self.replica_counter[key]>1:
						data_to_be_updated[key] = self.data[key]
						counters_to_be_updated[key] = self.replica_counter[key] - 1
						print("Passing on (", key,",",value,",",counters_to_be_updated[key],") as well")

			# send message now to successors:
			msg = [[self.get_id(), self.get_counter(), 10], [new_node_ID, data_to_be_updated, counters_to_be_updated, self.get_id()]]
			msg = pickle.dumps(msg,-1)
			self.get_successor()[2].send(msg)
			# print()

			return

	def update_data_on_depart(self, sent_data, sent_key, departing_node_id):
		if self.get_k() != 1:
			if sent_data or sent_key:
				for key,value in sent_data.items():
					if self.check_if_in_data(key) and self.get_data_replica_counter(key) == self.get_k():
						continue
					self.insert_data(key,value,1)
				sent_data = {}
				new_sent_key = set()
				for key in sent_key:
					current_counter = self.get_data_replica_counter(key)
					if current_counter == 1:
						sent_data[key] = self.get_data(key)
					else:
						new_sent_key.add(key)
					if self.get_data_replica_counter(key) == self.get_k():
						new_sent_key.remove(key)
					else:
						self.incr_data_replica_counter(key)

			if departing_node_id != self.get_successor()[2] and (sent_data or new_sent_key):
				msg = [[self.get_id(), self.get_counter(), 9], [sent_data, new_sent_key, departing_node_id]]
				msg = pickle.dumps(msg,-1)
				self.get_successor()[2].send(msg)
			print("Someone left, i am filling up on leftovers")
			return
		else:
			print("Someone left, i am filling up on leftovers")
			for key, value in sent_data.items():
				self.insert_data(key,value,1)
			return

	def overlay(self, list_of_nodes):
		# made a full circle, that means print out the whole DHT
		if self.get_id() == list_of_nodes[0] and len(list_of_nodes) > 1:
			print("The ring of nodes of the Chord is as follows:")
			print()
			# starting node has been added twice
			del list_of_nodes[0]
			print(list_of_nodes)
			sorted_list_of_nodes = list_of_nodes.copy()
			sorted_list_of_nodes.sort()
			print("\t          \t\t Sorted Chord:")
			if sorted_list_of_nodes[0] == list_of_nodes[0]:
				print("\t","\033[4m"+ str(list_of_nodes[0]) + "\033[0m", "⬅----⬉\t\t", "\033[4m"+ str(sorted_list_of_nodes[0]) + "\033[0m", "⬅----⬉")
			else:
				print("\t","\033[4m"+ str(list_of_nodes[0]) + "\033[0m", "⬅----⬉\t\t", sorted_list_of_nodes[0], "⬅----⬉")
			print("\t"," ↓  ", "     |\t\t ↓  ", "      |")
			for i in range(1,len(list_of_nodes) - 1):
				if sorted_list_of_nodes[i] != list_of_nodes[0]:
					print("\t",list_of_nodes[i], "     |\t\t", sorted_list_of_nodes[i], "     |")
				else:
					print("\t",list_of_nodes[i], "     |\t\t", "\033[4m"+ str(sorted_list_of_nodes[i]) + "\033[0m", "     |")
				print("\t"," ↓  ", "     |\t\t ↓  ", "      |")

			if sorted_list_of_nodes[-1] != list_of_nodes[0]:
				print("\t",list_of_nodes[-1], "➡----⬈\t\t", sorted_list_of_nodes[-1], "➡----⬈")
			else:
				print("\t",list_of_nodes[-1], "➡----⬈\t\t", "\033[4m"+ str(sorted_list_of_nodes[-1]) + "\033[0m", "➡----⬈")

		# add myself to the list
		else:
			list_of_nodes.append(self.get_id())
			msg = [[self.get_id(), self.get_counter(), 11], [list_of_nodes]]
			msg = pickle.dumps(msg,-1)
			self.get_successor()[2].send(msg)
		return

	def create_socket(self, ip_address, port):
		print(f"creating socket for address {ip_address} and port {port}")
		client_socket = server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		client_socket.connect((ip_address, port))
		self.set_counter()
		# set connection to non-blocking state, so .recv() call won't block, just return some exception we'll handle
		# if blocking was set to True that would mean wait until something is received
		client_socket.setblocking(False)
		# inform successor on port
		return client_socket
