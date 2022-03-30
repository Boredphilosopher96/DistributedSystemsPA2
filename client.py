from interface import ClientSuperNodeInterface, ttypes, ClientNodeInterface
from utils import CONFIG, get_client

if __name__ == '__main__':
    supernode_client: ClientSuperNodeInterface.Client = get_client(
        CONFIG['superNodeIp'], port=CONFIG['superNodePort'], client_class=ClientSuperNodeInterface.Client
    )

    node_info: ttypes.NodeInfo = supernode_client.get_node_for_client()

    # node_client: ClientNodeInterface.Client = get_client(
    #     node_info.ip_address, node_info.port_no, client_class=ClientNodeInterface.Client
    # )

    node_client: ClientNodeInterface.Client = get_client(
        '10.0.30.0', 5002, client_class=ClientNodeInterface.Client
    )

    node_client.put('Stunning', 'Beautiful, Gorgeous')
    node_client.put('Lassez-Faire', 'Lax attitude')

    print(node_client.get('Stunning', use_cache=False))
    print(node_client.get('Guru', use_cache=True))
