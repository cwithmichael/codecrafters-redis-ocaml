(lang dune 3.16)

(name codecrafters_redis)

(generate_opam_files true)

(package
 (name codecrafters_redis)
 (maintainers "CodeCrafters <hello@codecrafters.io>")
 (authors "CodeCrafters <hello@codecrafters.io>")
 (synopsis "Starter code for the Build your own Redis challenge")
 (homepage "https://codecrafters.io")
 (depends "base64" "lwt" "lwt_ppx" "ptime" "ppx_deriving")
 (bug_reports
  "https://github.com/codecrafters-io/build-your-own-redis/issues")
 (license "MIT")
 (allow_empty true)
 (version 0.1))
