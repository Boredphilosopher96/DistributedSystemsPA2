from threading import Thread

from interface import ClientSuperNodeInterface, ttypes, ClientNodeInterface
from utils import CONFIG, get_client


def put_words(word, meaning, client: ClientNodeInterface.Client):
    client.put(word, meaning)


if __name__ == '__main__':
    supernode_client: ClientSuperNodeInterface.Client = get_client(
        CONFIG['superNodeIp'], port=CONFIG['superNodePort'], client_class=ClientSuperNodeInterface.Client
    )

    node_info: ttypes.NodeInfo = supernode_client.get_node_for_client()

    node_client: ClientNodeInterface.Client = get_client(
        node_info.ip_address, node_info.port_no, client_class=ClientNodeInterface.Client
    )
    # words = {}
    # with open('dictionary_sample.txt') as file:
    #     for line in file:
    #         line = line.strip()
    #         if not line.startswith('Defn'):
    #             key = line.strip()
    #         else:
    #             words[key] = line.replace("Defn:", "").strip()
    #
    # print(words)

    # node_client: ClientNodeInterface.Client = get_client(
    #     '10.0.30.0', 5002, client_class=ClientNodeInterface.Client
    # )
    # threads = []
    # for word, meaning in words.items():
    #     t = Thread(target=put_words, args=(word, meaning, node_client))
    #     threads.append(t)
    #     t.start()
    #
    # for thread in threads:
    #     thread.join()
    # for word, meaning in words.items():
    #     node_client.put(word, meaning)

    # node_client.put('Ram', 'Catty')
    # node_client.put('Raj', 'Power')
    try:
        print(node_client.get('ARGL', use_cache=False))
        print(node_client.get('BARKEN', use_cache=False))
    except ttypes.CustomException as e:
        print(f"{e.message}")
