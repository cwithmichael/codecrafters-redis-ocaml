let ( let* ) = Lwt.bind

let create_client host port =
  let* addr_info =
    Lwt_unix.getaddrinfo host port [ Unix.(AI_FAMILY PF_INET) ]
  in
  let* sockaddr =
    match addr_info with
    | [] -> Lwt.fail_with "Bad host data"
    | addr :: _ -> Lwt.return addr.Unix.ai_addr
  in
  let client_socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let* () = Lwt_unix.connect client_socket sockaddr in
  let input = Lwt_io.of_fd ~mode:Lwt_io.input client_socket in
  let output = Lwt_io.of_fd ~mode:Lwt_io.output client_socket in
  Lwt.return (input, output)

let connect_to_master replicaof =
  try
    let addr = String.split_on_char ' ' !replicaof in
    let* inp, out = create_client (List.hd addr) (List.nth addr 1) in
    Lwt.return_some (inp, out)
  with _exn -> Lwt.return_none

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
