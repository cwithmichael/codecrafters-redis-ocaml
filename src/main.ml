open Config

let ( let* ) = Lwt.bind

let handle_client (input, output) config_data =
  let rec handle_command () =
    let buf = Bytes.make 1024 '0' in
    let* n = Lwt_io.read_into input buf 0 1024 in
    match n with
    | 0 -> Lwt_io.printl "Client disconnected"
    | size ->
        let res = Redis.parse_redis_input (Bytes.sub buf 0 size) 0 in
        let* () =
          Lwt_io.write output (Redis.encode_redis_value config_data res)
        in
        handle_command ()
  in
  handle_command ()

let rec accept_connections server_socket config_data =
  let* client_socket, _addr = Lwt_unix.accept server_socket in
  let input = Lwt_io.of_fd ~mode:Lwt_io.input client_socket in
  let output = Lwt_io.of_fd ~mode:Lwt_io.output client_socket in
  Lwt.async (fun () -> handle_client (input, output) config_data);
  accept_connections server_socket config_data

let start_server port config_data =
  let sockaddr = Unix.(ADDR_INET (inet_addr_any, port)) in
  let server_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in

  Lwt_unix.setsockopt server_socket Unix.SO_REUSEADDR true;
  let* () = Lwt_unix.bind server_socket sockaddr in
  Lwt_unix.listen server_socket 10;
  let* () = Lwt_io.printlf "Server started on port %d" port in
  accept_connections server_socket config_data

let dir = ref ""
let dbfilename = ref ""
let port = ref 6379

let speclist =
  [
    ("--port", Arg.Set_int port, "Set port");
    ("--dir", Arg.Set_string dir, "Output dir");
    ("--dbfilename", Arg.Set_string dbfilename, "Set db output file name");
  ]

let read_file_to_string file =
  let ic =
    try open_in_bin file
    with Sys_error msg -> failwith ("Cannot open file: " ^ msg)
  in
  let len = in_channel_length ic in
  let buf = really_input_string ic len in
  close_in ic;
  buf

let char_to_hex c =
  let byte_value = Char.code c in
  Printf.sprintf "%02x" byte_value

let process_kv filename =
  let kv = ref "" in
  let binary_string = read_file_to_string filename in
  let* () =
    Lwt_list.iteri_s
      (fun idx c ->
        let hex = char_to_hex c in
        let next_hex =
          if idx + 1 < String.length binary_string then
            char_to_hex (String.get binary_string (idx + 1))
          else ""
        in
        if hex = "00" then (
          match int_of_string_opt ("0x" ^ next_hex) with
          | Some len ->
              if
                len > 0
                && len < String.length binary_string
                && idx + 2 < String.length binary_string
              then kv := !kv ^ String.sub binary_string (idx + 2) len ^ "\t";
              Lwt.return ()
          | None ->
              kv := !kv ^ " " ^ next_hex;
              Lwt.return ())
        else Lwt.return ())
      (String.to_seq binary_string |> List.of_seq)
  in
  Lwt.return !kv

let main () =
  Arg.parse speclist (fun _ -> ()) "";
  let* kv =
    try%lwt process_kv (!dir ^ "/" ^ !dbfilename)
    with exn ->
      let msg =
        Printf.sprintf "Error processing KV file: %s\n" (Printexc.to_string exn)
      in
      Lwt.return msg
  in
  let* () = Lwt_io.printf "kv: %s" kv in
  let m =
    ConfigMap.(
      empty |> add "dir" !dir |> add "dbfilename" !dbfilename |> add "kv" kv)
  in
  start_server !port m

let () = Lwt_main.run (main ())
