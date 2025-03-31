module ConfigMap = Map.Make (String)

let queue : string list Queue.t = Queue.create ()
let queue_lock = Mutex.create ()
