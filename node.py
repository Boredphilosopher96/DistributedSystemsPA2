import hashlib
import math
import socket

from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

import utils
from interface import NodeInterface, ClientNodeInterface, ttypes, NodeSuperNodeInterface

SERVER_ID = 16


class NodeHandler:
    def __init__(self, max_dht_nodes, local_hostname=socket.gethostbyname(socket.gethostname()),
                 port_number=utils.CONFIG['superNodePort'] + SERVER_ID):
        self.finger_table = {}
        self.max_dht_nodes = max_dht_nodes
        self.meaning = {}
        self.cached_meaning = {}
        self.successor = None
        self.predecessor = None
        self.node_id = None
        # self.local_host = local_hostname
        self.local_host = "10.0.30.0"
        self.port_number = port_number

    def ping(self):
        print("Starting here")
        client = utils.get_client(
            utils.CONFIG['superNodeIp'], utils.CONFIG['superNodePort'], client_class=NodeSuperNodeInterface.Client
        )
        print(client.ping())

    def initiate_registration_with_supernode(self):
        node_supernode_client: NodeSuperNodeInterface.Client = utils.get_client(
            utils.CONFIG['superNodeIp'], utils.CONFIG['superNodePort'], client_class=NodeSuperNodeInterface.Client
        )

        # random_node: ttypes.NodeInfo = node_supernode_client.get_node_for_join(self.local_host, self.port_number)
        random_node: ttypes.NodeInfo = node_supernode_client.get_node_for_join(self.local_host, self.port_number)

        # If this is the first node joining the DHT, no need to update any finger tables
        if random_node.node_id is None:
            # If supernode did not assign a valid id, raise exception
            if (random_node.assigned_node_id is None) or (random_node.assigned_node_id not in range(0, self.max_dht_nodes)):
                raise ttypes.CustomException("The supernode did not assign a valid node id to the given node")

            # Else assign node id to this node
            self.node_id = random_node.assigned_node_id

            current_node = ttypes.NodeInfo(node_id=self.node_id, ip_address=self.local_host, port_no=self.port_number)

            # Since this is the only node in the DHT, it is the successor and predecessor for everything
            for node_id in self.get_finger_node_ids(self.node_id):
                self.finger_table[node_id] = current_node

            self.predecessor = current_node
            self.set_successor(current_node)

            # Signal to supernode that join is complete
            node_supernode_client.post_join(random_node.assigned_node_id)
            print(f"Node initialized with the following details {current_node} and finger table {self.finger_table}")
        else:
            # Make sure that the node assigned is valid
            if random_node.assigned_node_id is None or random_node.assigned_node_id not in range(0, self.max_dht_nodes):
                raise ttypes.CustomException("The supernode did not assign a valid node id to the given node")

            self.node_id = random_node.assigned_node_id

            random_node_client: NodeInterface.Client = utils.get_client(ip_address=random_node.ip_address, port=random_node.port_no,
                                                                        client_class=NodeInterface.Client)
            successor_node: ttypes.NodeInfo = random_node_client.find_successor(self.node_id)
            if successor_node.node_id == self.node_id:
                successor_node_client = self
            else:
                successor_node_client: NodeInterface.Client = utils.get_client(ip_address=successor_node.ip_address,
                                                                               port=successor_node.port_no,
                                                                               client_class=NodeInterface.Client)

            predecessor_node: ttypes.NodeInfo = successor_node_client.get_predecessor()
            if predecessor_node.node_id != self.node_id:
                predecessor_node_client: NodeInterface.Client = utils.get_client(ip_address=predecessor_node.ip_address,
                                                                                 port=predecessor_node.port_no,
                                                                                 client_class=NodeInterface.Client)
            else:
                predecessor_node_client = self

            self.set_successor(successor_node)
            self.predecessor = predecessor_node
            current_node = ttypes.NodeInfo(node_id=self.node_id, ip_address=self.local_host, port_no=self.port_number)

            predecessor_node_client.set_successor(current_node)
            successor_node_client.set_predecessor(current_node)

            # Set the full finger table
            finger_ids = self.get_finger_node_ids(self.node_id)
            self.finger_table[finger_ids[0]] = self.get_successor()

            for index, node_id in enumerate(finger_ids[1:]):
                if self.check_if_in_between(node_id, self.node_id, self.finger_table[finger_ids[index]].node_id, start_inclusive=True):
                    self.finger_table[node_id] = self.finger_table[finger_ids[index]]
                else:
                    if random_node.node_id == self.get_predecessor().node_id:
                        self.finger_table[node_id] = current_node
                    else:
                        self.finger_table[node_id] = random_node_client.find_successor(node_id)

            # Ask other nodes to update finger tables as well
            self._ask_others_to_update_finger_tables()

            for index, node_id in enumerate(finger_ids[1:]):
                if self.check_if_in_between(node_id, self.node_id, self.finger_table[finger_ids[index]].node_id, start_inclusive=True):
                    self.finger_table[node_id] = self.finger_table[finger_ids[index]]
                else:
                    if random_node.node_id == self.get_predecessor().node_id:
                        self.finger_table[node_id] = current_node
                    else:
                        self.finger_table[node_id] = random_node_client.find_successor(node_id)

            node_supernode_client.post_join(self.node_id)

            print(f"Node initialized with the following details {current_node}")

    def _ask_others_to_update_finger_tables(self):
        for i in range(math.ceil(math.log2(self.max_dht_nodes))):
            hashed_id = ((self.node_id - 2 ** i) % self.max_dht_nodes)
            # if the modulus is negative, wrap around and check the distance from the other direction
            if hashed_id < 0:
                hashed_id += self.max_dht_nodes

            # Ask the predecessor of the hashed id to update it's ith node
            predecessor_node: ttypes.NodeInfo = self.find_predecessor(hashed_id)
            if predecessor_node.node_id != self.node_id:
                predecessor_node_client: NodeInterface.Client = utils.get_client(ip_address=predecessor_node.ip_address,
                                                                                 port=predecessor_node.port_no,
                                                                                 client_class=NodeInterface.Client)
            else:
                predecessor_node_client = self

            predecessor_node_client.update_finger_table(i,
                                                        ttypes.NodeInfo(self.node_id, ip_address=self.local_host,
                                                                        port_no=self.port_number))

    def hash_word(self, word: str) -> int:
        """
        Encode the word using MD5 hashing algorithm

        :param word: Word that is supposed to be hashed
        :return: hashed id modulo max nodes in dht
        """
        hashed_word = hashlib.md5(word.encode())
        return int(hashed_word.hexdigest(), 16) % self.max_dht_nodes

    @staticmethod
    def check_if_in_between(hashed_id, possible_predecessor, possible_successor, start_inclusive=False, end_inclusive=False):
        if possible_predecessor is None or possible_successor is None:
            raise ttypes.CustomException("One of the successor or predecessor is none")
        # In case of wrap predecessor and successor are same, it usually means there is one node. So hashed_id is always in between
        if possible_predecessor == possible_successor:
            return True

        # In case predecessor is less than successor, that means there is no wrap around in DHT. We just need to verify hashed_id is
        # in the space between
        elif possible_predecessor < possible_successor:
            is_in_between = True
            if start_inclusive:
                is_in_between = is_in_between and possible_predecessor <= hashed_id
            else:
                is_in_between = is_in_between and possible_predecessor < hashed_id

            if end_inclusive:
                is_in_between = is_in_between and hashed_id <= possible_successor
            else:
                is_in_between = is_in_between and hashed_id < possible_successor

            return is_in_between

        # In case there is wrap around
        else:
            # In case hashed id is also wrapped around
            if possible_predecessor >= hashed_id and hashed_id <= possible_successor:
                is_in_between = True
                if start_inclusive:
                    is_in_between = is_in_between and possible_predecessor >= hashed_id
                else:
                    is_in_between = is_in_between and possible_predecessor > hashed_id

                if end_inclusive:
                    is_in_between = is_in_between and hashed_id <= possible_successor
                else:
                    is_in_between = is_in_between and hashed_id < possible_successor

                return is_in_between

            # In case only successor is wrapped around
            elif possible_predecessor <= hashed_id and hashed_id >= possible_successor:
                is_in_between = True
                if start_inclusive:
                    is_in_between = is_in_between and possible_predecessor <= hashed_id
                else:
                    is_in_between = is_in_between and possible_predecessor < hashed_id

                if end_inclusive:
                    is_in_between = hashed_id >= possible_successor
                else:
                    is_in_between = hashed_id > possible_successor

                return is_in_between

            else:
                # Handle default case
                return False

    def find_successor(self, hashed_id: int) -> ttypes.NodeInfo:
        if self.node_id == hashed_id:
            return ttypes.NodeInfo(node_id=self.node_id, ip_address=self.local_host, port_no=self.port_number)
        predecessor = self.find_predecessor(hashed_id)
        node_client: NodeInterface.Client = utils.get_client(predecessor.ip_address, predecessor.port_no, NodeInterface.Client)
        return node_client.get_successor()

    def get_successor(self) -> ttypes.NodeInfo:
        return self.successor

    def get_predecessor(self) -> ttypes.NodeInfo:
        return self.predecessor

    def find_predecessor(self, hashed_id: int) -> ttypes.NodeInfo:
        closest_predecessor = self.get_closest_predecessor(hashed_id)
        if closest_predecessor.node_id == self.node_id:
            return closest_predecessor

        predecessor_client: NodeInterface.Client = utils.get_client(
            ip_address=closest_predecessor.ip_address, port=closest_predecessor.port_no,
            client_class=NodeInterface.Client
        )
        while not self.check_if_in_between(hashed_id, closest_predecessor.node_id, predecessor_client.get_successor().node_id,
                                           end_inclusive=True):
            closest_predecessor = predecessor_client.get_closest_predecessor(hashed_id)
            predecessor_client: NodeInterface.Client = utils.get_client(
                ip_address=closest_predecessor.ip_address, port=closest_predecessor.port_no,
                client_class=NodeInterface.Client
            )

        return closest_predecessor

    def get_finger_node_ids(self, node_id):
        finger_node_ids = []
        max_bits = math.ceil(math.log2(self.max_dht_nodes))
        # max_bits = len(self.finger_table)
        for i in range(max_bits):
            finger_node_ids.append((node_id + 2 ** i) % self.max_dht_nodes)
        return finger_node_ids

    def get_closest_predecessor(self, hashed_id: int) -> ttypes.NodeInfo:
        for node_id in reversed(self.get_finger_node_ids(self.node_id)):
            if self.check_if_in_between(self.finger_table[node_id].node_id, self.node_id, hashed_id):
                return self.finger_table[node_id]

        return ttypes.NodeInfo(node_id=self.node_id, ip_address=self.local_host, port_no=self.port_number)

    def set_predecessor(self, node_info: ttypes.NodeInfo):
        self.predecessor = node_info

    def set_successor(self, node_info: ttypes.NodeInfo):
        # self.finger_table[(self.node_id + 1) % self.max_dht_nodes] = node_info
        self.successor = node_info

    def update_finger_table(self, id: int, node_info: ttypes.NodeInfo):
        if node_info.node_id == self.node_id:
            return
        finger_nodes = self.get_finger_node_ids(self.node_id)
        # Try to match this with paper and other people's implementation
        if self.check_if_in_between(node_info.node_id, self.node_id, self.finger_table[finger_nodes[id]].node_id, start_inclusive=True):
            self.finger_table[finger_nodes[id]] = node_info
            # if self.get_predecessor().node_id == node_info.node_id:
            #     return
            if self.get_predecessor().node_id != self.node_id and self.get_predecessor().node_id != node_info.node_id:
                predecessor_client: NodeInterface.Client = utils.get_client(
                    ip_address=self.get_predecessor().ip_address, port=self.get_predecessor().port_no,
                    client_class=NodeInterface.Client
                )
                predecessor_client.update_finger_table(id, node_info)

    def put(self, word: str, meaning: str) -> bool:
        """
        Put a word into the DHT to be read later

        :param word: word to be put in the DHT
        :param meaning: Meaning of the word to be stored in DHT
        :return: a True boolean. This returns a boolean only to signal to the caller that insertion is successful
        """
        print(f"Putting the meaning of the word {word}")
        # Always keep cache of every word that goes through
        self.cached_meaning[word] = meaning
        hashed_id = self.hash_word(word)

        # If this is the successor node to the hashed id, store it in this node
        if self.check_if_in_between(hashed_id, self.get_predecessor().node_id, self.node_id, end_inclusive=True):
            print(f"Putting the meaning of the word : {word} in this node")
            self.meaning[word] = meaning
            return True
        else:
            print(f"The word {word} is being searched for in the DHT")
            # Else find the successor, and store the data there
            successor: ttypes.NodeInfo = self.find_successor(hashed_id)
            successor_client: NodeInterface.Client = utils.get_client(ip_address=successor.ip_address, port=successor.port_no,
                                                                      client_class=NodeInterface.Client)
            return successor_client.put(word, meaning)

    def get(self, word: str, use_cache: bool = False) -> ttypes.Result:
        """
        Get the meaning of the word in the DHT. If meaning does not exist in the current node, it contacts the node which has the meaning
        and tries to get the meaning

        :param word: word for which we want to know the meaning of
        :param use_cache: boolean representing can the node use cached value of meaning?
        :return: a result type object with meaning of the word and the IP of node called by client and the node containing the word
        :raises: A custom exception. This can happen when the client asks for meaning of word that is not in DHT
        """
        print(f"Getting meaning for word {word}")

        # If we use cache, and the word is cached, we return the word
        if use_cache and word in self.cached_meaning:
            print(f"Getting cached meaning for word {word}")
            return ttypes.Result(answer=self.cached_meaning[word], path=[f"{self.node_id}"])

        # Hash the word to know which node the data should be stored in
        hashed_id = self.hash_word(word)

        # Check if this is the node responsible for storing the data. It will be true if this node is the successor to hashed_id
        if self.check_if_in_between(hashed_id, self.get_predecessor().node_id, self.node_id, end_inclusive=True):
            # if we are at the correct node and the data is not stored here, that means the data is not found in the DHT
            if word not in self.meaning:
                raise ttypes.CustomException("The given word you are searching for is not in the DHT")

            # If it is found, we attach the local IP and the meaning of the word
            return ttypes.Result(answer=self.meaning[word], path=[self.node_id])
        else:
            # If data is not found in the current node, we find the successor node and task it to get the data
            successor: ttypes.NodeInfo = self.find_successor(hashed_id)
            successor_client: NodeInterface.Client = utils.get_client(ip_address=successor.ip_address, port=successor.port_no,
                                                                      client_class=NodeInterface.Client)
            result: ttypes.Result = successor_client.get(word, use_cache)
            # Attach the path of the current node tasking it as well
            result.path.append(self.node_id)
            return result


if __name__ == '__main__':
    handler = NodeHandler(utils.CONFIG['maxNodes'])
    # Using multiplexed processor because this same server has 2 thrift interfaces
    processor = TMultiplexedProcessor()
    # Registering the 2 different processors which this server serves
    processor.registerProcessor(
        utils.CONFIG["multiplexingKeys"][NodeInterface.Client.__module__],
        NodeInterface.Processor(handler)
    )
    processor.registerProcessor(
        utils.CONFIG["multiplexingKeys"][ClientNodeInterface.Client.__module__], ClientNodeInterface.Processor(handler)
    )

    transport = TSocket.TServerSocket(port=utils.CONFIG['superNodePort'] + SERVER_ID)

    transport_factory = TTransport.TBufferedTransportFactory()
    protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, transport_factory, protocol_factory)

    print("Initiating registration with supernode")
    handler.initiate_registration_with_supernode()
    print('Starting the node')
    server.serve()
