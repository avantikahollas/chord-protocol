import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result
import messages.{
  type Msg, FixFingers, Init, InitChord, Join, NodeInfo, Result, SendRequests,
  Start,
}
import node

pub type AggregateState {
  AggregateState(
    num_nodes: Int,
    num_requests: Int,
    m: Int,
    total_hops: Int,
    node_ack_count: Int,
    agg_subject: Subject(Msg),
  )
}

pub fn handle(
  state: AggregateState,
  msg: Msg,
) -> actor.Next(AggregateState, Msg) {
  case msg {
    Start(agg_subject) -> {
      let new_state =
        AggregateState(
          state.num_nodes,
          state.num_requests,
          state.m,
          state.total_hops,
          state.node_ack_count,
          agg_subject,
        )

      //create min nodes and initialize chord structure
      let nodes = [1, 8, 32]
      let node_info_list =
        list.index_map(nodes, fn(x, _i) {
          let node_state =
            node.NodeState(
              NodeInfo(x, process.new_subject()),
              32,
              dict.from_list([]),
              [],
              option.None,
              NodeInfo(x, process.new_subject()),
              state.num_requests,
              agg_subject,
            )
          let assert Ok(node_actor) =
            actor.new(node_state)
            |> actor.on_message(node.handle)
            |> actor.start()

          // echo #(x, node_actor.data)
          NodeInfo(x, node_actor.data)
        })
      list.each(node_info_list, fn(n) { actor.send(n.subject, Init(n.subject)) })

      let first_node_info =
        result.unwrap(
          list.first(node_info_list),
          NodeInfo(1, process.new_subject()),
        )
      //create a chord network
      actor.send(first_node_info.subject, InitChord(node_info_list))
      list.each(node_info_list, fn(n) { actor.send(n.subject, FixFingers) })

      //Create additional nodes and have them join the chord network
      let joined_nodes_list =
        list.range(1, state.num_nodes - 3)
        |> list.index_map(fn(_, _id) {
          let node_id = int.random(int_pow(2, state.m))
          // let node_id = 2734149526
          let node_state =
            node.NodeState(
              NodeInfo(node_id, process.new_subject()),
              32,
              dict.from_list([]),
              [],
              option.None,
              NodeInfo(node_id, process.new_subject()),
              state.num_requests,
              agg_subject,
            )
          let assert Ok(node_actor) =
            actor.new(node_state)
            |> actor.on_message(node.handle)
            |> actor.start()
          NodeInfo(node_id, node_actor.data)
        })

      list.each(joined_nodes_list, fn(n) {
        actor.send(n.subject, Init(n.subject))
        actor.send(first_node_info.subject, Join(NodeInfo(n.id, n.subject)))
      })
      process.sleep(2000)
      let nodes_list = list.append(node_info_list, joined_nodes_list)
      list.each(nodes_list, fn(n) { actor.send(n.subject, FixFingers) })
      process.sleep(10_000)
      list.each(nodes_list, fn(node_info) {
        actor.send(node_info.subject, SendRequests)
      })
      actor.continue(new_state)
    }

    Result(hops) -> {
      let new_total_hops = state.total_hops + hops
      let new_node_ack_count = state.node_ack_count + 1
      // echo "Total hops so far: " <> int.to_string(new_total_hops)
      // echo "Node ack count so far: " <> int.to_string(new_node_ack_count)
      case new_node_ack_count == state.num_nodes * state.num_requests {
        True -> {
          let average_hops =
            new_total_hops / { state.num_nodes * state.num_requests }
          io.println("All nodes have completed their requests.")
          io.println(
            "Average hops per request: " <> int.to_string(average_hops),
          )
          actor.stop()
        }

        False -> {
          actor.continue(AggregateState(
            state.num_nodes,
            state.num_requests,
            state.m,
            new_total_hops,
            new_node_ack_count,
            state.agg_subject,
          ))
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
