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
  | BulkString of string option * int
  | RedisArray of string option list * int
  | RedisInt of int * int
  | SimpleString of string option * int
  | NullBulkString

let parse_int buf pos =
  let num_str, next_pos = read_until_crlf buf pos in
  RedisInt (int_of_string num_str, next_pos)

let parse_bulk_string buf pos =
  match parse_int buf pos with
  | RedisInt (len, next_pos) ->
      if len = -1 then BulkString (None, next_pos)
      else
        let str = Bytes.sub_string buf next_pos len in
        BulkString (Some str, next_pos + len + 2)
  | _ -> NullBulkString

let parse_simple_string buf pos =
  let str, next_pos = read_until_crlf buf pos in
  SimpleString (Some str, next_pos)

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

let delay_execution f timeout =
  let* () = Lwt_unix.sleep timeout in
  f ()

let handle_set_sub cmd v key =
  match cmd with
  | Some _ -> (
      (*only handle px for now*)
      match v with
      | Some delay ->
          delay_execution
            (fun () ->
              Mutex.lock dict_mutex;
              Hashtbl.remove dict key;
              Mutex.unlock dict_mutex;
              Lwt.return ())
            delay
      | None -> Lwt.return_unit)
  | None -> failwith "Unsupported subcommand"

let create_bulk_string data =
  match data with
  | None -> "$-1\r\n"
  | Some str -> Printf.sprintf "$%d\r\n%s\r\n" (String.length str) str

let create_array_of_bulk_string data =
  List.fold_left
    (fun acc x -> acc ^ create_bulk_string x)
    (Printf.sprintf "*%d\r\n" (List.length data))
    data

let check_for_redis_command input config_data =
  match List.hd input with
  | Some cmd -> (
      match String.lowercase_ascii cmd with
      | "ping" -> Some (SimpleString (Some "PONG", 0))
      | "echo" -> (
          match List.nth_opt input 1 with
          | Some (Some s) -> Some (SimpleString (Some s, 0))
          | _ -> failwith "Invalid input for echo")
      | "config" -> (
          match List.nth_opt input 2 with
          | Some None -> failwith ""
          | Some (Some s) -> (
              match String.lowercase_ascii s with
              | "dir" ->
                  let dir = ConfigMap.find_opt "dir" config_data in
                  Some (RedisArray ([ Some "dir"; dir ], -1))
              | "dbfilename" ->
                  let dbfilename =
                    ConfigMap.find_opt "dbfilename" config_data
                  in
                  Some (RedisArray ([ Some "dbfilename"; dbfilename ], -1))
              | _ -> failwith "Invalid input for config")
          | None -> failwith "")
      | "set" -> (
          match
            ( List.nth_opt input 1,
              List.nth_opt input 2,
              List.nth_opt input 3,
              List.nth_opt input 4 )
          with
          | Some (Some key), Some (Some value), None, None ->
              update_dict key value;
              Some (SimpleString (Some "OK", 0))
          | Some (Some key), Some (Some value), Some sub_cmd, Some sub_cmd_val
            ->
              (* only handle px for now*)
              update_dict key value;
              Lwt.ignore_result
                (handle_set_sub sub_cmd
                   (match Option.map float_of_string sub_cmd_val with
                   | Some delay -> Some (delay /. 1000.0)
                   | None -> failwith "Invalid subcommand value")
                   key);
              Some (SimpleString (Some "OK", 0))
          | _ -> failwith "Invalid input for set")
      | "get" -> (
          match List.nth_opt input 1 with
          | Some (Some key) -> (
              match Hashtbl.find_opt dict key with
              | None -> Some NullBulkString
              | Some v -> Some (BulkString (Some v, -1)))
          | _ -> failwith "Invalid input for set")
      | _ -> failwith @@ "Unsupported command " ^ cmd)
  | None -> None

let rec encode_redis_value config_data = function
  | RedisInt (i, _) -> Printf.sprintf "%d\r\n" i
  | NullBulkString -> "$-1\r\n"
  | BulkString (None, _) -> "$-1\r\n"
  | BulkString (Some str, d) -> (
      if d = -1 then create_bulk_string (Some str)
      else
        match check_for_redis_command [ Some str ] config_data with
        | Some s -> encode_redis_value config_data s
        | None -> "$-1\r\n")
  | SimpleString (None, _) -> "$-1\r\n"
  | SimpleString (Some str, _) -> Printf.sprintf "+%s\r\n" str
  | RedisArray (elems, d) -> (
      if d = -1 then create_array_of_bulk_string elems
      else
        match check_for_redis_command elems config_data with
        | Some s -> encode_redis_value config_data s
        | None -> failwith "Bad array data")
