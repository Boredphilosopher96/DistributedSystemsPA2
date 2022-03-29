import hashlib
import socket

from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

import utils
from interface import NodeInterface, ClientNodeInterface, ttypes, NodeSuperNodeInterface


class NodeHandler:
    def __init__(self, max_dht_nodes, local_hostname = socket.getfqdn(socket.gethostname()), port_number = 5000):
        self.finger_table = {}
        self.max_dht_nodes = max_dht_nodes
        self.meaning = {}
        self.cached_meaning = {}
        self.successor = None
        self.predecessor = None
        self.node_id = None
        self.local_hostname = local_hostname
        self.port_number = port_number
        # self.ping()
        self.initiate_registration_with_supernode()
    
    def ping(self):
        print("Starting here")
        client = utils.get_client(
            utils.CONFIG['superNodeIp'], utils.CONFIG['superNodePort'], client_class = NodeSuperNodeInterface.Client
        )
        print(client.ping())
    
    def initiate_registration_with_supernode(self):
        node_supernode_client: NodeSuperNodeInterface.Client = utils.get_client(
            utils.CONFIG['superNodeIp'], utils.CONFIG['superNodePort'], client_class = NodeSuperNodeInterface.Client
        )
        
        # random_node: ttypes.NodeInfo = node_supernode_client.get_node_for_join(self.local_hostname, self.port_number)
        random_node: ttypes.NodeInfo = node_supernode_client.get_node_for_join("10.0.30.0", self.port_number)
        
        # If this is the first node joining the DHT, no need to update any finger tables
        if random_node.node_id is None:
            # If supernode did not assign a valid id, raise exception
            if (random_node.assigned_node_id is None) or (
                random_node.assigned_node_id not in range(0, self.max_dht_nodes)):
                raise ttypes.CustomException("The supernode did not assign any node id to the given node")
            
            # Else assign node id to this node and tell supernode join is complete
            self.node_id = random_node.assigned_node_id
            node_supernode_client.post_join(random_node.assigned_node_id)
        else:
            pass
    
    def hash_word(self, word: str) -> int:
        hashed_word = hashlib.md5(word.encode())
        return int(hashed_word.hexdigest(), 16) % self.max_dht_nodes
    
    @staticmethod
    def check_if_in_between(hashed_id, possible_predecessor, possible_successor):
        if possible_predecessor < possible_successor:
            return possible_predecessor <= hashed_id < possible_successor
        return possible_predecessor <= hashed_id or hashed_id < possible_successor
    
    def get_successor(self, hashed_id: int) -> ttypes.NodeInfo:
        if hashed_id == self.node_id:
            if self.successor is not None:
                return self.successor
            else:
                raise ttypes.CustomException("Making request to this node before all finger tables are updated")
        else:
            pass
    
    def get_predecessor(self, hashed_id: int) -> ttypes.NodeInfo:
        pass
    
    def get_closest_predecessor(self, hashed_id: int) -> ttypes.NodeInfo:
        pass
    
    def set_predecessor(self, node_info: ttypes.NodeInfo):
        pass
    
    def set_successor(self, node_info: ttypes.NodeInfo):
        pass
    
    def update_finger_table(self, id: int, node_info: ttypes.NodeInfo):
        pass
    
    def put(self, word: str, meaning: str) -> bool:
        pass
    
    def get(self, word: str, use_cache: bool = False) -> ttypes.Result:
        # If we use cache, and the word is cached, we return the word
        if use_cache and word in self.cached_meaning:
            print(f"Getting cached meaning for word {word}")
            return ttypes.Result(answer = self.cached_meaning[word], path = [f"{self.node_id}"])
        pass


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
    
    transport = TSocket.TServerSocket(host = "10.0.30.0", port = utils.CONFIG['superNodePort'])
    
    transport_factory = TTransport.TBufferedTransportFactory()
    protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadPoolServer(processor, transport, transport_factory, protocol_factory)
    
    print('Starting the node')
    server.serve()
