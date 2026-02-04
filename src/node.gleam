import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import messages.{
  type Msg, type NodeInfo, type NodeMsg, FindFinger, FindSuccessor, FixFingers,
  Init, InitChord, Join, NodeInfo, Search, SendRequests, SetFinger, SetSuccessor,
}

pub type NodeState {
  NodeState(
    id: NodeInfo,
    m: Int,
    finger_table: Dict(Int, NodeInfo),
    store: List(#(Int, Int)),
    predecessor: Option(NodeInfo),
    successor: NodeInfo,
    num_requests: Int,
    aggregate_subject: Subject(Msg),
  )
}

pub fn handle(state: NodeState, msg: NodeMsg) -> actor.Next(NodeState, NodeMsg) {
  case msg {
    Init(subject) -> {
      // Initialize the node with its subject and create finger table
      let finger_list =
        list.index_map(list.range(0, state.m - 1), fn(_, i) {
          let finger_id = { state.id.id + int_pow(2, i) } % int_pow(2, state.m)
          let finger_info = NodeInfo(finger_id, subject)
          // echo #(finger_id, finger_info)
          #(finger_id, finger_info)
        })
      // echo finger_list
      let new_state =
        NodeState(
          NodeInfo(state.id.id, subject),
          state.m,
          dict.from_list(finger_list),
          state.store,
          state.predecessor,
          NodeInfo(state.id.id, subject),
          state.num_requests,
          state.aggregate_subject,
        )
      actor.continue(new_state)
    }

    SetSuccessor(successor) -> {
      let new_state =
        NodeState(
          state.id,
          state.m,
          state.finger_table,
          state.store,
          state.predecessor,
          successor,
          state.num_requests,
          state.aggregate_subject,
        )
      // echo #(
      //   new_state.id.id,
      //   new_state.id.subject,
      //   new_state.successor.id,
      //   new_state.successor.subject,
      // )
      actor.continue(new_state)
    }

    InitChord(node_info_list) -> {
      // echo "Initializing Chord network"
      let first = case list.first(node_info_list) {
        Ok(n) -> n
        Error(_) -> NodeInfo(0, process.new_subject())
      }
      init_chord(node_info_list, first)
      actor.continue(state)
    }

    FindSuccessor(node) -> {
      case node.id > state.id.id && node.id <= state.successor.id {
        True -> {
          // echo #(node.id, state.successor.id)
          actor.send(node.subject, SetSuccessor(state.successor))
          actor.send(state.id.subject, SetSuccessor(node))
        }
        False -> {
          case state.id.id >= state.successor.id {
            True -> {
              actor.send(node.subject, SetSuccessor(state.successor))
              actor.send(state.id.subject, SetSuccessor(node))
            }
            False -> actor.send(state.successor.subject, FindSuccessor(node))
          }
        }
      }
      actor.continue(state)
    }

    Join(node) -> {
      // echo "Node " <> int.to_string(node.id) <> " joining the network"
      actor.send(state.id.subject, FindSuccessor(node))
      actor.continue(state)
    }

    FixFingers -> {
      // echo "Fixing fingers for node " <> int.to_string(state.id.id)
      list.each(dict.to_list(state.finger_table), fn(finger) {
        let #(finger_id, _finger_info) = finger
        actor.send(state.id.subject, FindFinger(finger_id, state.id.subject))
      })
      actor.continue(state)
    }

    FindFinger(id, curr) -> {
      // Find the closest finger for the given id
      case id > state.id.id && id <= state.successor.id {
        True -> {
          actor.send(curr, SetFinger(id, state.successor))
        }
        False -> {
          case state.id.id >= state.successor.id {
            True -> actor.send(curr, SetFinger(id, state.successor))
            False -> actor.send(state.successor.subject, FindFinger(id, curr))
          }
        }
      }
      actor.continue(state)
    }

    SetFinger(index, successor) -> {
      let new_finger_table = dict.insert(state.finger_table, index, successor)
      let new_state =
        NodeState(
          state.id,
          state.m,
          new_finger_table,
          state.store,
          state.predecessor,
          state.successor,
          state.num_requests,
          state.aggregate_subject,
        )

      actor.continue(new_state)
    }

    SendRequests -> {
      // Send a number of requests to random keys
      // echo "Node " <> int.to_string(state.id.id) <> " sending requests"
      list.each(list.range(1, state.num_requests), fn(_) {
        let key = int.random(int_pow(2, state.m / 4) - 1)
        actor.send(state.id.subject, Search(key, 0))
      })
      actor.continue(state)
    }

    Search(key, hops) -> {
      case key > state.id.id && key <= state.successor.id {
        True -> {
          actor.send(state.aggregate_subject, messages.Result(hops + 1))
          actor.continue(state)
        }
        False -> {
          case state.id.id >= state.successor.id {
            True -> {
              actor.send(state.aggregate_subject, messages.Result(hops + 1))
              actor.continue(state)
            }
            False -> {
              closest_preceding_node(
                list.reverse(dict.to_list(state.finger_table)),
                key,
                hops,
                state.id,
                state.successor,
              )
              actor.continue(state)
            }
          }
        }
      }
      actor.continue(state)
    }
  }
}

pub fn init_chord(remaining: List(NodeInfo), first_node: NodeInfo) -> Nil {
  case remaining {
    [] -> Nil
    [current, ..rest] -> {
      case rest {
        [] -> {
          actor.send(
            current.subject,
            SetSuccessor(NodeInfo(first_node.id, first_node.subject)),
          )
          init_chord(rest, first_node)
        }
        [next, ..] -> {
          actor.send(
            current.subject,
            SetSuccessor(NodeInfo(next.id, next.subject)),
          )
          init_chord(rest, first_node)
        }
      }
    }
  }
}

pub fn int_pow(base: Int, exp: Int) -> Int {
  case exp {
    0 -> 1
    _ -> base * int_pow(base, exp - 1)
  }
}

pub fn closest_preceding_node(
  finger_list: List(#(Int, NodeInfo)),
  key: Int,
  hops: Int,
  node_info: NodeInfo,
  successor: NodeInfo,
) -> Nil {
  case finger_list {
    [] -> actor.send(successor.subject, Search(key, hops + 1))
    [finger, ..rest] -> {
      let #(finger_id, finger_info) = finger
      case key <= finger_id && key > node_info.id {
        True -> {
          actor.send(finger_info.subject, Search(key, hops + 1))
          Nil
        }
        False -> closest_preceding_node(rest, key, hops, node_info, successor)
      }
    }
  }
}
