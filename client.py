from interface import ClientSuperNodeInterface, ttypes, ClientNodeInterface
from utils import CONFIG, get_client

if __name__ == '__main__':
    supernode_client: ClientSuperNodeInterface.Client = get_client(
        CONFIG['superNodeIp'], port=CONFIG['superNodePort'], client_class=ClientSuperNodeInterface.Client
    )

    node_info: ttypes.NodeInfo = supernode_client.get_node_for_client()

    node_client: ClientNodeInterface.Client = get_client(
        node_info.ip_address, node_info.port_no, client_class=ClientNodeInterface.Client
    )

    # node_client: ClientNodeInterface.Client = get_client(
    #     '10.0.30.0', 5002, client_class=ClientNodeInterface.Client
    # )

    node_client.put('Lion', 'Catty')
    node_client.put('Sea', 'Power')

    print(node_client.get('Sea', use_cache=False))
    print(node_client.get('Lion', use_cache=False))
