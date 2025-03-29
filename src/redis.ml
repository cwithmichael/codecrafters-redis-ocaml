open Config

let ( let* ) = Lwt.bind

let read_until_crlf buf pos =
  let rec aux i =
    if
      i + 1 < Bytes.length buf
      && Bytes.get buf i = '\r'
      && Bytes.get buf (i + 1) = '\n'
    then (Bytes.sub_string buf pos (i - pos), i + 2)
    else aux (i + 1)
  in
  aux pos

type redis_value =
  | BulkString of string * int
  | RedisArray of string list * int
  | RedisInt of int * int
  | SimpleString of string * int
  | NullBulkString

let parse_int buf pos =
  let num_str, next_pos = read_until_crlf buf pos in
  RedisInt (int_of_string num_str, next_pos)

let parse_bulk_string buf pos =
  match parse_int buf pos with
  | RedisInt (len, next_pos) ->
      if len = -1 then NullBulkString
      else
        let str = Bytes.sub_string buf next_pos len in
        BulkString (str, next_pos + len + 2)
  | _ -> NullBulkString

let parse_simple_string buf pos =
  let str, next_pos = read_until_crlf buf pos in
  SimpleString (str, next_pos)

let rec parse_array buf pos =
  match parse_int buf pos with
  | RedisInt (len, next_pos) ->
      let rec aux n p acc =
        if n = 0 then RedisArray (List.rev acc, p)
        else
          let result = parse_value buf p in
          match result with
          | BulkString (elem, pos) -> aux (n - 1) pos (elem :: acc)
          | _ -> failwith "unsupported"
      in
      aux len next_pos []
  | _ -> failwith "unsupported"

and parse_value buf pos : redis_value =
  match Bytes.get buf pos with
  | '+' -> parse_simple_string buf (pos + 1)
  | '$' -> parse_bulk_string buf (pos + 1)
  | '*' -> parse_array buf (pos + 1)
  | ':' -> parse_int buf (pos + 1)
  | _ -> failwith "Unsupported type"

let parse_redis_input buf pos =
  match Bytes.get buf pos with
  | '-' -> failwith "Unsupported type"
  | '$' | '*' | '+' | ':' -> parse_value buf pos
  | _ -> failwith "Invalid input"

let dict = Hashtbl.create 10
let dict_mutex = Mutex.create ()

let update_dict key value =
  Mutex.lock dict_mutex;
  Hashtbl.add dict key value;
  Mutex.unlock dict_mutex

let delete_from_dict key =
  Mutex.lock dict_mutex;
  Hashtbl.remove dict key;
  Mutex.unlock dict_mutex

let delay_execution f timeout =
  let* () = Lwt_unix.sleep @@ timeout in
  f ()

let handle_set_sub cmd v key =
  match cmd with
  | Some _ -> (
      (*only handle px for now*)
      match v with
      | Some delay ->
          let* () = Lwt_io.printf "Scheduled to remove %s : %f\n" key delay in
          let delay = delay /. 1000. in
          let tm = Unix.gmtime delay in
          let cur_tm = Unix.gmtime (Unix.time ()) in
          (* figure out a better check than this smh *)
          if delay > 100000. && tm.tm_year < cur_tm.tm_year then (
            delete_from_dict key;
            Lwt.return ())
          else
            delay_execution
              (fun () ->
                delete_from_dict key;
                Lwt.return ())
              delay
      | None -> Lwt.return_unit)
  | None -> failwith "Unsupported subcommand"

let create_bulk_string data =
  match data with
  | None -> "$-1\r\n"
  | Some str ->
      if String.length str < 1 then ""
      else Printf.sprintf "$%d\r\n%s\r\n" (String.length str) str

let create_array_of_bulk_string data =
  List.fold_left
    (fun acc x -> acc ^ create_bulk_string (Some x))
    (Printf.sprintf "*%d\r\n" (List.length data))
    data

let handle_ping = Some (SimpleString ("PONG", 0))

let handle_echo input =
  match List.nth_opt input 1 with
  | Some s -> Some (SimpleString (s, 0))
  | _ -> failwith "Invalid input for echo"

let handle_config input config_data =
  match List.nth_opt input 2 with
  | Some s -> (
      match String.lowercase_ascii s with
      | "dir" ->
          let dir = List.hd @@ ConfigMap.find "dir" config_data in
          Some (RedisArray ([ "dir"; dir ], -1))
      | "dbfilename" ->
          let dbfilename = List.hd @@ ConfigMap.find "dbfilename" config_data in
          Some (RedisArray ([ "dbfilename"; dbfilename ], -1))
      | _ -> failwith @@ "Unknown sub command for config " ^ s)
  | None -> failwith "Invalid input for config"

let handle_set input =
  match
    ( List.nth_opt input 1,
      List.nth_opt input 2,
      List.nth_opt input 3,
      List.nth_opt input 4 )
  with
  | Some key, Some value, None, None ->
      update_dict key value;
      Some (SimpleString ("OK", 0))
  | Some key, Some value, Some sub_cmd, Some sub_cmd_val ->
      (* only handle px for now*)
      update_dict key value;
      let x = sub_cmd_val |> Int64.of_string |> Int64.to_float in
      Lwt.ignore_result (handle_set_sub (Some sub_cmd) (Some x) key);
      Some (SimpleString ("OK", 0))
  | _ -> failwith "Invalid input for set"

let handle_get input =
  match List.nth_opt input 1 with
  | Some key -> (
      match Hashtbl.find_opt dict key with
      | None -> Some NullBulkString
      | Some v -> Some (BulkString (v, -1)))
  | _ -> failwith "Invalid input for get"

let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

let handle_info _ config_data =
  match ConfigMap.find "replicaof" config_data with
  | [ "" ] ->
      let info_data =
        Printf.sprintf "role:master\r\nmaster_replid:%s\r\nmaster_repl_offset:0"
          repl_id
      in
      Some (BulkString (info_data, -1))
  | _ -> Some (BulkString ("role:slave", -1))

let handle_replconf _ _config_data = Some (SimpleString ("OK", -1))

let handle_psync _ _config_data =
  let resp = Printf.sprintf "FULLRESYNC %s 0" repl_id in
  Some (SimpleString (resp, -1))

let check_for_redis_command input config_data =
  match List.nth_opt input 0 with
  | Some cmd -> (
      match String.lowercase_ascii cmd with
      | "psync" -> handle_psync input config_data
      | "replconf" -> handle_replconf input config_data
      | "info" -> handle_info input config_data
      | "ping" -> handle_ping
      | "echo" -> handle_echo input
      | "config" -> handle_config input config_data
      | "set" -> handle_set input
      | "get" -> handle_get input
      | "keys" -> (
          match ConfigMap.find_opt "keys" config_data with
          | Some keys ->
              let keys = keys |> Util.filter_empty_strings in
              Some (RedisArray (keys, -1))
          | None -> Some NullBulkString)
      | _ -> failwith @@ "Unsupported command " ^ cmd)
  | None -> None

let rec encode_redis_value config_data = function
  | RedisInt (i, _) -> Printf.sprintf "%d\r\n" i
  | NullBulkString -> "$-1\r\n"
  | BulkString (str, d) -> (
      if d = -1 then create_bulk_string (Some str)
      else
        match check_for_redis_command [ str ] config_data with
        | Some s -> encode_redis_value config_data s
        | None -> "$-1\r\n")
  | SimpleString (str, _) -> Printf.sprintf "+%s\r\n" str
  | RedisArray (elems, d) -> (
      if d = -1 then create_array_of_bulk_string elems
      else
        match check_for_redis_command elems config_data with
        | Some s -> encode_redis_value config_data s
        | None -> failwith "Bad array data")
