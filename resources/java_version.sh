#!/usr/bin/env bash

set -e

MINIMUM_JAVA_VERSION=11

# path to java executable argument
JAVA_X=$1

# optional maximum version argument
MAXIMUM_JAVA_VERSION=$2

if [[ "$JAVA_X" ]]; then
  JAVA_VERSION=$("$JAVA_X" -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' 'OFS="." {print $1,$2}')
  if [[ "$JAVA_VERSION" == "1."* ]]; then
    JAVA_VERSION=${JAVA_VERSION#*.}
  else
    JAVA_VERSION=${JAVA_VERSION%.*}
  fi
  if (( JAVA_VERSION < MINIMUM_VERSION )); then
    echo "FlureeDB requires minimum Java ${MINIMUM_VERSION}. Your version is: $JAVA_VERSION. Exiting."
    exit 1
  elif [[ -n "${MAXIMUM_VERSION}" ]] && (( JAVA_VERSION > MAXIMUM_VERSION )); then
    echo "FlureeDB requires maximum Java ${MAXIMUM_VERSION}. Your version is: $JAVA_VERSION. Exiting."
    exit 1
  else
    echo "Java version $JAVA_VERSION."
  fi
else
  echo "Usage: java_version.sh /path/to/java_executable [maximum major java version]"
  exit 1
fi
