open Config

type redis_value =
  | BulkString of string * int
  | RedisArray of string list * int
  | RedisInt of int * int
  | SimpleString of string * int
  | NullBulkString
[@@deriving show]

type decoded_redis_type = Int of int | String of string [@@deriving show]

val encode_redis_value :
  ?config:string list ConfigMap.t ->
  ?conn:(Lwt_io.input_channel * Lwt_io.output_channel) option ->
  redis_value ->
  string
(** Encodes the redis value to a RESP string *)

val decode_redis_value : redis_value -> decoded_redis_type

val parse_redis_input : bytes -> int -> redis_value
(** Parses input data as RESP input *)

val handle_set : string list -> redis_value option
(** Set kv in dict*)
