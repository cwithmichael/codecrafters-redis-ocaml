let ( let* ) = Lwt.bind

let handle_client (input, output) =
  let rec handle_command () =
    let buf = Bytes.make 1024 '0' in
    let* n = Lwt_io.read_into input buf 0 1024 in
    match n with
    | 0 -> Lwt_io.printl "Client disconnected"
    | size ->
        let res = Redis.parse_redis_input (Bytes.sub buf 0 size) 0 in
        let* () = Lwt_io.write output (Redis.encode_redis_value res) in
        handle_command ()
  in
  handle_command ()

let rec accept_connections server_socket =
  let* client_socket, _addr = Lwt_unix.accept server_socket in
  let input = Lwt_io.of_fd ~mode:Lwt_io.input client_socket in
  let output = Lwt_io.of_fd ~mode:Lwt_io.output client_socket in
  Lwt.async (fun () -> handle_client (input, output));
  accept_connections server_socket

let start_server port =
  let sockaddr = Unix.(ADDR_INET (inet_addr_any, port)) in
  let server_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in

  Lwt_unix.setsockopt server_socket Unix.SO_REUSEADDR true;
  let* () = Lwt_unix.bind server_socket sockaddr in
  Lwt_unix.listen server_socket 10;
  let* () = Lwt_io.printlf "Server started on port %d" port in
  accept_connections server_socket

let () =
  let port = 6379 in
  Lwt_main.run (start_server port)
