#!/usr/bin/env bash

set -e

image=fluree/${PWD##*/}

builder_image=${image}:builder

# output build stdout to /dev/null b/c even with --quiet it still outputs
# sha256:blahblahblah and we sometimes want to consume the docker run output
# in another script
docker build --quiet --target builder --tag "${builder_image}" . >/dev/null
docker run --rm ${DOCKER_RUN_ARGS} "${builder_image}" "$@"
