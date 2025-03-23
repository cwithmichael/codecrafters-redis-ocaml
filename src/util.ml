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
