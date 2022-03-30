import datetime
import random

from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

import utils
from interface import ClientSuperNodeInterface, NodeSuperNodeInterface, ttypes


class SuperNodeHandler:
    def __init__(self, max_nodes_in_dht):
        self._node_list = []
        self.node_details = {}
        self._dht_size = max_nodes_in_dht
        # This serves as a mutex to ensure that the system does not allow for more than one node to be added at a time
        self.__is_new_node_being_added: bool = False
        self.__node_being_added = None
        self.__node_add_started = None

    def get_node_for_client(self) -> ttypes.NodeInfo:
        print("Getting random node for client")
        random_node_id = self._get_random_node()
        print(f"Got node with id: {random_node_id} : details : {self.node_details[random_node_id]}")
        return self.node_details[random_node_id]

    def ping(self) -> str:
        print("Pinging")
        return "ping ping ping"

    def __reset_supernode(self):
        self.__node_add_started = None
        self.__node_being_added = None
        self.__is_new_node_being_added = False

    def _check_if_timed_out(self):
        if self.__node_add_started is None:
            self.__reset_supernode()
            raise ttypes.CustomException("Node add time not recorded. So supernode has been reset")
        if datetime.datetime.now() - self.__node_add_started > datetime.timedelta(seconds=30):
            print("Previous node timed out. So it is removed from being added")
            self.__reset_supernode()
            return True
        return False

    def _get_random_node(self) -> int:
        return random.choice(self._node_list)

    def get_node_for_join(self, ip: str, port_no: int) -> ttypes.NodeInfo:
        try:
            print("Calling new node to join")
            if self.__is_new_node_being_added:
                if not self._check_if_timed_out():
                    # Because currently a node is being added
                    raise ttypes.CustomException("NACK")

            self.__is_new_node_being_added = True

            # If we have max number of nodes, we don't have any empty key left in hash space. So we raise exception
            if len(self._node_list) >= self._dht_size:
                self.__reset_supernode()
                raise ttypes.CustomException("The maximum number of nodes are already attached in DHT")

            self.__node_add_started = datetime.datetime.now()

            # Make sure supernode generates a key not already in DHT
            while True:
                node_id = random.randint(0, self._dht_size - 1)
                if node_id not in self.node_details:
                    break

            # If this node is the first node joining the DHT, we return a node with id None which lets node know
            # that it is the first node
            if not self._node_list:
                print(f"This is the first node joining. It is assigned ID {node_id}")
                self.__node_being_added = ttypes.NodeInfo(node_id=node_id, ip_address=ip, port_no=port_no)
                return ttypes.NodeInfo(assigned_node_id=node_id)

            random_node_id = self._get_random_node()
            random_node = ttypes.NodeInfo(
                node_id=random_node_id, ip_address=self.node_details[random_node_id].ip_address,
                port_no=self.node_details[random_node_id].port_no, assigned_node_id=node_id
            )

            # Store the details of noe currently being added in a temp variable
            self.__node_being_added = ttypes.NodeInfo(node_id=node_id, ip_address=ip, port_no=port_no)
            print(f"Adding node with id {node_id} and ip address {ip}")
            return random_node

        except Exception as e:
            print(f"{e}")
            print(f"Could not add node {self.__node_being_added}")
            self.__is_new_node_being_added = False
            self.__node_being_added = None

    def post_join(self, node_id: int):
        print(f"Post join is called by {node_id}")
        # If we don't have a node currently being added to DHT, something has gone wrong. So raise exception
        if self.__node_being_added is None:
            self.__is_new_node_being_added = False
            raise ttypes.CustomException("No node currently being executed")
        # Node calling post join is not the same node which started joining
        elif self.__node_being_added.node_id != node_id:
            raise ttypes.CustomException("Node calling post join did not initiate join")

        # Add node to our list of nodes and release the "mutex lock"
        self.node_details[node_id] = self.__node_being_added
        self._node_list.append(node_id)
        self.__reset_supernode()
        print(f"Node {node_id} joined DHT")


if __name__ == '__main__':
    handler = SuperNodeHandler(utils.CONFIG['maxNodes'])
    # Because it implements multiple interfaces, register all the processors with name given in config file
    processor = TMultiplexedProcessor()
    processor.registerProcessor(
        utils.CONFIG["multiplexingKeys"][ClientSuperNodeInterface.Client.__module__],
        ClientSuperNodeInterface.Processor(handler)
    )
    processor.registerProcessor(
        utils.CONFIG["multiplexingKeys"][NodeSuperNodeInterface.Client.__module__],
        NodeSuperNodeInterface.Processor(handler)
    )

    transport = TSocket.TServerSocket(host=utils.CONFIG['superNodeIp'], port=utils.CONFIG['superNodePort'])

    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print('Starting the supernode')
    server.serve()
