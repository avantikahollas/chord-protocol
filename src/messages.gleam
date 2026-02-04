import gleam/erlang/process.{type Subject}

pub type Msg {
  Start(Subject(Msg))
  Result(Int)
}

pub type NodeMsg {
    Init(subject: Subject(NodeMsg))
    InitChord(node_info_list: List(NodeInfo))
    SetSuccessor(successor: NodeInfo)
    FindSuccessor(node_info: NodeInfo)
    FindFinger(id: Int, requester: Subject(NodeMsg))
    SetFinger(index: Int, successor: NodeInfo)
    Join(node_info: NodeInfo)
    FixFingers()
    SendRequests()
    Search(key: Int, hops: Int)
}

pub type NodeInfo {
  NodeInfo(
    id: Int,
    subject: Subject(NodeMsg),
  )
}
