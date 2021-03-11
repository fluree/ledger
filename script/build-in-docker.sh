#!/usr/bin/env bash

set -e

script_path=$(realpath "$0")
script_dir=$(dirname "$script_path")

DOCKER_RUN_ARGS="--volume ${script_dir}/../build:/usr/src/fluree-ledger/build:rw"
export DOCKER_RUN_ARGS

"${script_dir}"/run-in-docker.sh "$@"
