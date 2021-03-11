#!/usr/bin/env bash

set -e

image=fluree/${PWD##*/}

builder_image=${image}:builder

echo "Running in ${builder_image} container..."

docker build --quiet --target builder --tag "${builder_image}" .
docker run --rm ${DOCKER_RUN_ARGS} "${builder_image}" "$@"
