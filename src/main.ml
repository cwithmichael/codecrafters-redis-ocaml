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

let process_kv filename =
  let keys = ref "" in
  let values = ref "" in
  let binary_string = Util.read_file_to_string filename in
  let* () =
    Lwt_list.iteri_s
      (fun idx c ->
        let hex = Util.char_to_hex c in
        let next_hex =
          if idx + 1 < String.length binary_string then
            Util.char_to_hex (String.get binary_string (idx + 1))
          else ""
        in
        if hex = "00" then
          match int_of_string_opt ("0x" ^ next_hex) with
          | Some len ->
              if
                len > 0
                && len < String.length binary_string
                && idx + 2 < String.length binary_string
              (* clean this up with refactor *)
              then (
                keys := !keys ^ String.sub binary_string (idx + 2) len ^ "\t";
                match
                  int_of_string_opt
                    ("0x"
                    ^ Util.char_to_hex
                        (String.get binary_string (idx + 2 + len)))
                with
                | Some vlen ->
                    values :=
                      !values
                      ^ String.sub binary_string (idx + 2 + len + 1) vlen
                      ^ "\t";
                    Lwt.return ()
                | None -> Lwt.return ())
              else Lwt.return ()
          | None -> Lwt.return ()
        else Lwt.return ())
      (String.to_seq binary_string |> List.of_seq)
  in
  Lwt.return (!keys, !values)

let main () =
  Arg.parse speclist (fun _ -> ()) "";
  let* keys, values =
    try%lwt process_kv (!dir ^ "/" ^ !dbfilename)
    with exn ->
      let msg =
        Printf.sprintf "Error processing KV file: %s\n" (Printexc.to_string exn)
      in
      Lwt.return ("", msg)
  in
  let* () = Lwt_io.printf "kv: %s %s" keys values in
  let m =
    ConfigMap.(
      empty |> add "dir" !dir
      |> add "dbfilename" !dbfilename
      |> add "keys" keys |> add "values" values)
  in

  let keys = String.split_on_char '\t' keys |> Util.filter_empty_strings in
  let values = String.split_on_char '\t' values |> Util.filter_empty_strings in
  List.iteri
    (fun i k -> ignore @@ Redis.handle_set [ ""; k; List.nth values i ])
    keys;
  start_server !port m

let () = Lwt_main.run (main ())
