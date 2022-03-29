exception CustomException {
  1: optional string message
}

// This struct was created to keep track of node information.
// Assigned node id is optional because it will only be the case when a new node wants to join DHT
struct NodeInfo {
    1: i32 node_id,
    2: string ip_address,
    3: i32 port_no,
    4: optional i32 assigned_node_id
}

// This is the struct containing result from get
// Info of each node the DHT calls is added to path
struct Result {
    1: string answer,
    2: list<string> path
}

service ClientNodeInterface {
    bool put(1:string word, 2:string meaning),

    Result get(1:string word, 2:bool use_cache) throws (1: CustomException customException)
}


service ClientSuperNodeInterface {
    NodeInfo get_node_for_client() throws (1: CustomException customException)
}


service NodeSuperNodeInterface {
    NodeInfo get_node_for_join(1: string ip, 2: i32 port_no) throws (1: CustomException customException),

    string ping(),

    void post_join(1: i32 node_id)
}

service NodeInterface {
    NodeInfo get_successor(),

    NodeInfo find_successor(1: i32 node_id),

    NodeInfo get_predecessor(),

    NodeInfo find_predecessor(1: i32 node_id),

    NodeInfo get_closest_predecessor(1: i32 node_id),

    void set_predecessor(1: NodeInfo node_info),

    void set_successor(1: NodeInfo node_info),

    void update_finger_table(1: i32 id, 2: NodeInfo node_info),

    bool put(1: string word, 2: string meaning)

    Result get(1:string word, 2:bool use_cache) throws (1: CustomException customException)

}