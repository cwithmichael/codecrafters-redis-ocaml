open Config

type redis_value

val encode_redis_value : string ConfigMap.t -> redis_value -> string
(** Encodes the redis value to a RESP string *)

val parse_redis_input : bytes -> int -> redis_value
(** Parses input data as RESP input *)

val handle_set : string list -> redis_value option
(** Set kv in dict*)
