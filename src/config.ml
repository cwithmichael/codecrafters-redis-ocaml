module Config = struct
  type t = string

  let compare x y = Stdlib.compare x y
end

module ConfigMap = Map.Make (Config)
