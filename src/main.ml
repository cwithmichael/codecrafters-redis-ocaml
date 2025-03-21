let ( let* ) = Lwt.bind

let start_server port =
  let sockaddr = Unix.(ADDR_INET (inet_addr_any, port)) in
  let server_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in

  Lwt_unix.setsockopt server_socket Unix.SO_REUSEADDR true;
  let* () = Lwt_unix.bind server_socket sockaddr in
  Lwt_unix.listen server_socket 10;
  let* () = Lwt_io.printlf "Server started on port %d" port in
  let* _sock, _addr = Lwt_unix.accept server_socket in
  Lwt.return_unit

let () =
  let port = 6379 in
  Lwt_main.run (start_server port)
