import aggregate.{AggregateState}
import argv
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/otp/actor
import messages.{Start}

pub fn main() -> Nil {
  case argv.load().arguments {
    [num_nodes_str, num_requests_str] -> {
      case int.parse(num_nodes_str), int.parse(num_requests_str) {
        Ok(num_nodes), Ok(num_requests) -> {
          io.println(
            "Starting Chord with "
            <> int.to_string(num_nodes)
            <> " nodes, "
            <> int.to_string(num_requests)
            <> " requests each",
          )
          let aggregate_state =
            AggregateState(
              num_nodes,
              num_requests,
              32,
              0,
              0,
              process.new_subject(),
            )

          let assert Ok(aggregate_actor) =
            actor.new(aggregate_state)
            |> actor.on_message(aggregate.handle)
            |> actor.start()
          let aggregate_subject = aggregate_actor.data
          actor.send(aggregate_subject, Start(aggregate_subject))
          wait(aggregate_actor.pid)
        }
        _, _ ->
          io.println(
            "Error: both arguments must be integers. Usage: program <num_nodes> <num_requests>",
          )
      }
    }
    _ -> io.println("Usage: program <num_nodes> <num_requests>")
  }
}

pub fn wait(process_pid: process.Pid) -> Nil {
  case process.is_alive(process_pid) {
    True -> {
      wait(process_pid)
    }
    False -> {
      io.println("Exiting program.")
    }
  }
}
