#!/bin/sh
#
# This script is used to run your program on CodeCrafters
#
# This runs after .codecrafters/compile.sh
#
# Learn more: https://codecrafters.io/program-interface

set -e # Exit on failure

OCAMLRUNPARAM=b exec /tmp/codecrafters-build-redis-ocaml/default/main.exe "$@"
