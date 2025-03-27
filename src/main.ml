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

let decode_length data pos =
  let b = int_of_char @@ Bytes.get data pos in
  let flag = (b land 0xc0) lsr 6 in
  let six_bits = b land 0x3f in
  Printf.printf "idx: %d b:%d flag: %d six: %d\n" pos b flag six_bits;

  match flag with
  | 0 -> (six_bits, 0)
  | 1 ->
      let next_b = Char.code @@ Bytes.get data (pos + 1) in
      ((six_bits lsl 8) lor next_b, 1)
  | 2 -> (Int32.to_int (Bytes.get_int32_le data (pos + 1)), 4)
  | 3 -> (six_bits, 1)
  | _ ->
      failwith (Printf.sprintf "data:%s\n pos: %d" (Bytes.to_string data) pos)

let decode_int data pos =
  let b = int_of_char @@ Bytes.get data pos in
  let flag = (b land 0xc0) lsr 6 in
  Printf.printf "decoding int with flag: %d\n" flag;
  let decoded_value =
    match flag with
    | 0 -> 1
    | 1 -> 2
    | 2 -> 4
    | _ ->
        failwith (Printf.sprintf "Invalid integer encoding at position %d" pos)
  in
  (decoded_value, pos + decoded_value)

let get_kv data idx keys values timestamp =
  try
    let kl, skip1 = decode_length data !idx in
    let key_start = !idx + skip1 in
    let key = Bytes.sub data (key_start + 1) kl in
    let vl, skip2 = decode_length data (!idx + 1 + kl) in
    let val_start = !idx + kl + skip1 + skip2 + 1 in
    Printf.printf
      "Before: idx: %d kl:%d skip1: %d key_st: %d vl:%d skip2:%d val_start:%d\n"
      !idx kl skip1 key_start vl skip2 val_start;
    let value = Bytes.sub data (val_start + 1) vl in
    idx := !idx + kl + vl + 2;
    keys := key :: !keys;
    values := value :: !values;
    Printf.printf "KEY: %s VALUE: %s\n" (String.of_bytes key)
      (String.of_bytes value);
    Printf.printf "After: idx: %d \n\n" !idx;
    let timestamp = match timestamp with Some t -> t | None -> 0L in
    (key, value, timestamp)
  with exn ->
    Printf.eprintf "Error processing KV: %s\n" (Printexc.to_string exn);
    (Bytes.of_string "", Bytes.of_string "", 0L)

let parse_redis_rdb filename =
  let data = Util.read_file_to_string filename |> Bytes.of_string in
  let idx = ref 0 in
  let keys = ref [] in
  let values = ref [] in
  let timestamps = ref [] in
  let result = ref [] in
  let new_db = ref false in
  while !idx < Bytes.length data do
    let opcode = Bytes.get data !idx in
    match opcode with
    | '\xFF' ->
        (* End of file *)
        new_db := false;
        idx := Bytes.length data
    | '\xFA' ->
        idx := !idx + 1;
        ignore @@ get_kv data idx keys values None
    | '\xFB' ->
        idx := !idx + 1;
        let _, new_pos = decode_int data !idx in
        idx := new_pos;
        let _, new_pos = decode_int data !idx in
        idx := new_pos;
        idx := !idx + 1;
        Printf.printf "New pos: %d\n" !idx;
        new_db := true
    | '\xFC' ->
        (* Expiry timestamp *)
        idx := !idx + 1;
        let ts = Bytes.get_int64_le data !idx in
        timestamps := ts :: !timestamps;
        idx := !idx + 8;
        let key, value, timestamp = get_kv data idx keys values (Some ts) in
        result := (key, value, timestamp) :: !result
    | '\xFE' ->
        idx := !idx + 1;
        let _, new_pos = decode_int data !idx in
        idx := new_pos
    | _ ->
        if !new_db = true then (
          let key, value, timestamp = get_kv data idx keys values None in
          if key <> Bytes.of_string "" then
            result := (key, value, timestamp) :: !result;
          idx := !idx + 1)
        else
          (* Unknown opcode, skip *)
          idx := !idx + 1
  done;
  !result

let main () =
  Arg.parse speclist (fun x -> Printf.printf "%s" x) "";

  let pairs =
    if !dbfilename <> "" then
      try parse_redis_rdb (!dir ^ "/" ^ !dbfilename) with _exn -> []
    else []
  in
  let keys =
    List.fold_left
      (fun acc (k, _, _) -> (String.of_bytes k |> String.trim) ^ "\t" ^ acc)
      "" pairs
  in
  let values =
    List.fold_left
      (fun acc (_, v, _) -> (String.of_bytes v |> String.trim) ^ " " ^ acc)
      "" pairs
  in
  let m =
    ConfigMap.(
      empty |> add "dir" !dir
      |> add "dbfilename" !dbfilename
      |> add "keys" keys |> add "values" values)
  in
  List.iter
    (fun (k, v, exp) ->
      let k = Bytes.to_string k in
      let v = Bytes.to_string v in
      if exp <= 0L then ignore @@ Redis.handle_set [ ""; k; v ]
      else ignore @@ Redis.handle_set [ ""; k; v; "PX"; Int64.to_string exp ])
    pairs;
  start_server !port m

let () = Lwt_main.run (main ())
