import json

from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport import TSocket, TTransport

from interface import ClientNodeInterface, ClientSuperNodeInterface, NodeInterface, NodeSuperNodeInterface
from interface.ttypes import CustomException

"""
This file was created to load the configuration files for the project
And to avoid writing repeated code
"""
try:
    with open('config.json') as _config:
        CONFIG = json.load(_config)
except FileNotFoundError as e:
    raise CustomException(message = "Config file not found")
except Exception as e:
    raise CustomException(f"Cannot load the config file:\n{e}")


def check_if_client_is_eligible(client_class) -> bool:
    eligible_classes = {ClientNodeInterface.Client,
                        ClientSuperNodeInterface.Client, NodeInterface.Client,
                        NodeSuperNodeInterface.Client}
    if client_class not in eligible_classes:
        raise CustomException("Trying to create illegal client")
    return True


def get_client(ip_address, port, client_class):
    if check_if_client_is_eligible(client_class):
        # Make socket
        transport = TSocket.TSocket(ip_address, port = port)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        
        transport = TTransport.TBufferedTransport(transport)
        
        if client_class.__module__ in CONFIG["multiplexingKeys"]:
            protocol = TMultiplexedProtocol.TMultiplexedProtocol(
                protocol, serviceName = CONFIG["multiplexingKeys"][client_class.__module__]
            )
        
        # Create a client to use the protocol encoder
        client = client_class(protocol)
        
        # Connect!
        transport.open()
        
        return client
