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

let filter_empty_strings strings =
  List.filter (fun s -> String.trim s <> "") strings

let xs_to_string xs =
  List.fold_left
    (fun acc x ->
      let k, v, exp = x in
      let exp = Int64.to_float exp /. 1000. in
      let k = Bytes.to_string k in
      let v = Bytes.to_string v in
      let time_str =
        Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d"
          (1900 + (Unix.localtime exp).Unix.tm_year)
          ((Unix.localtime exp).Unix.tm_mon + 1)
          (Unix.localtime exp).Unix.tm_mday (Unix.localtime exp).Unix.tm_hour
          (Unix.localtime exp).Unix.tm_min (Unix.localtime exp).Unix.tm_sec
      in
      " (" ^ k ^ ") : " ^ v ^ " : " ^ time_str ^ acc)
    " " xs
