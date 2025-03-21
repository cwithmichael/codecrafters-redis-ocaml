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

let parse_int buf pos =
  let num_str, next_pos = read_until_crlf buf pos in
  Printf.printf "%s\n" num_str;
  RedisInt (int_of_string num_str, next_pos)

let parse_bulk_string buf pos =
  match parse_int buf pos with
  | RedisInt (len, next_pos) ->
      if len = -1 then BulkString (None, next_pos)
      else
        let str = Bytes.sub_string buf next_pos len in
        BulkString (Some str, next_pos + len + 2)
  | _ -> failwith "Unsupported"

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

let check_for_redis_command input =
  match List.hd input with
  | Some cmd -> (
      match String.lowercase_ascii cmd with
      | "ping" -> Some "+PONG\r\n"
      | "echo" -> (
          match List.nth_opt input 1 with
          | Some (Some s) ->
              Some (Printf.sprintf "$%d\r\n%s\r\n" (String.length s) s)
          | Some None | None -> Some "$-1\r\n")
      | _ -> failwith "Unsupported command")
  | None -> None

let encode_redis_value = function
  | RedisInt (i, _) -> Printf.sprintf "%d\r\n" i
  | BulkString (None, _) -> "$-1\r\n"
  | BulkString (Some str, _) -> (
      match check_for_redis_command [ Some str ] with
      | Some s -> s
      | None -> "$-1\r\n")
  | SimpleString (None, _) -> "\r\n"
  | SimpleString (Some str, _) -> Printf.sprintf "+%s\r\n" str
  | RedisArray (elems, _) -> (
      match check_for_redis_command elems with
      | Some s ->
          Printf.printf "%s\n" s;
          s
      | None ->
          Printf.printf "%s\n" "LMAO";
          let encoded_elems =
            List.map (function None -> "$-1\r\n" | Some str -> str) elems
          in
          Printf.sprintf "*%d\r\n%s" (List.length elems)
            (String.concat "" encoded_elems))
