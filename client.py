import sys

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

    if len(sys.argv) > 1:
        words = {}
        with open(sys.argv[1]) as file:
            for line in file:
                line = line.strip()
                if not line.startswith('Defn'):
                    key = line.strip()
                else:
                    words[key] = line.replace("Defn:", "").strip()

        for word, meaning in words.items():
            node_client.put(word, meaning)

        while True:
            word = input("What word do you want to try to get?")
            try:
                print(node_client.get(word.strip(), use_cache=False))
            except ttypes.CustomException as e:
                print(str(e))

    else:
        try:
            node_client.put("Thor", "Hero")
            node_client.put("DHT", "Distributed hash table")
            print(f"meaning of the word thor - {node_client.get('thor', use_cache=False)}")
            print(f"meaning of the word dht - {node_client.get('dht', use_cache=False)}")
        except ttypes.CustomException as e:
            print(f"{e.message}")
