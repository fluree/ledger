#!/usr/bin/env bash

set -e

THIS_DIR="$(dirname "$0")"

CONFIG_PREFIXES=("/etc" "/usr/local/etc" "/opt/homebrew/etc" "/opt/etc")
JAR_PREFIXES=("/usr/share/java" "/usr/local/share/java" "/opt/homebrew/share/java" "/opt/share/java")

SYSTEM_JAR_DIR=${SYSTEM_JAR_DIR:-/usr/local/share/java}
SYSTEM_CONFIG_DIR=${SYSTEM_CONFIG_DIR:-/usr/local/etc}

FLUREE_LEDGER_JAR=fluree-ledger.standalone.jar
DEFAULT_PROPERTIES_FILE=fluree_sample.properties
PROPERTIES_FILE=fluree.properties

DEFAULT_LOGBACK_CONFIG_FILE=logback.xml
SYSTEM_LOGBACK_CONFIG_FILE=fluree-logback.xml

FLUREE_SERVER=""
FLUREE_PROPERTIES=""
FLUREE_LOGBACK_CONFIGURATION_FILE=""

MINIMUM_JAVA_VERSION=11

function find_java() {
  local JAVA_X
  if type -p java >/dev/null; then
    # found java executable in PATH
    JAVA_X=$(which java)
  elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
    # echo found java executable in JAVA_HOME
    JAVA_X="$JAVA_HOME/bin/java"
  else
    echo "Java is not installed or cannot be found. FlureeDB requires Java ${MINIMUM_JAVA_VERSION}+. If installed, check JAVA_HOME environment variable."
    exit 1
  fi

  echo "$JAVA_X"
}

function java_version() {
  # path to java executable argument
  local JAVA_X=$1
  if [ -z "${JAVA_X}" ]; then
    JAVA_X=$(find_java)
  else
    shift
  fi

  # optional maximum Java version argument
  local MAXIMUM_JAVA_VERSION=$1

  JAVA_VERSION=$("$JAVA_X" -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' 'OFS="." {print $1,$2}')
  if [[ "$JAVA_VERSION" == "1."* ]]; then
    JAVA_VERSION=${JAVA_VERSION#*.}
  else
    JAVA_VERSION=${JAVA_VERSION%.*}
  fi
  if ((JAVA_VERSION < MINIMUM_JAVA_VERSION)); then
    echo "FlureeDB requires minimum Java ${MINIMUM_JAVA_VERSION}. Your version is: $JAVA_VERSION. Exiting."
    exit 1
  elif [[ -n "${MAXIMUM_JAVA_VERSION}" ]] && ((JAVA_VERSION > MAXIMUM_JAVA_VERSION)); then
    echo "FlureeDB requires maximum Java ${MAXIMUM_JAVA_VERSION}. Your version is: $JAVA_VERSION. Exiting."
    exit 1
  else
    echo "Java version $JAVA_VERSION."
  fi
}

first_arg=$1

case "${first_arg}" in
find_java)
  shift
  find_java
  exit 0
  ;;
java_version)
  shift
  java_version "$@"
  exit 0
  ;;
esac

## Find Java executable
JAVA_X=$(find_java)

## Check Java Version
java_version "$JAVA_X"

function find_jar() {
  local jar_path="${SYSTEM_JAR_DIR}/${FLUREE_LEDGER_JAR}"
  if [ -f "$jar_path" ]; then
    echo "$jar_path"
    return 0
  fi
  for prefix in "${JAR_PREFIXES[@]}"; do
    jar_path="${prefix}/${FLUREE_LEDGER_JAR}"
    if [ -f "$jar_path" ]; then
      echo "$jar_path"
      return 0
    fi
  done
  echo "ERROR: Could not locate ${FLUREE_LEDGER_JAR} file. Exiting."
  exit 1
}

## decide if we're using local JAR or system-wide JAR
if [ -f ${THIS_DIR}/${FLUREE_LEDGER_JAR} ]; then
  FLUREE_SERVER="${THIS_DIR}/${FLUREE_LEDGER_JAR}"
else
  FLUREE_SERVER=$(find_jar)
fi

function find_properties_file() {
  local props_path="${SYSTEM_CONFIG_DIR}/${PROPERTIES_FILE}"
  if [ -f "$props_path" ]; then
    echo "$props_path"
    return 0
  fi
  for prefix in "${CONFIG_PREFIXES[@]}"; do
    props_path="${prefix}/${PROPERTIES_FILE}"
    if [ -f "$props_path" ]; then
      echo "$props_path"
      return 0
    fi
  done
}

## decide if we're using local properties file or system-wide
if [ -f "${THIS_DIR}/${PROPERTIES_FILE}" ]; then
  FLUREE_PROPERTIES="${THIS_DIR}/${PROPERTIES_FILE}"
else
  FLUREE_PROPERTIES=$(find_properties_file)
  SYSTEM_CONFIG_DIR=$(dirname "${FLUREE_PROPERTIES}")
fi

## decide if we're using local logback config file or system-wide
if [ -f "${THIS_DIR}/${DEFAULT_LOGBACK_CONFIG_FILE}" ]; then
  FLUREE_LOGBACK_CONFIGURATION_FILE="${THIS_DIR}/${DEFAULT_LOGBACK_CONFIG_FILE}"
elif [ -f "${SYSTEM_CONFIG_DIR}/${SYSTEM_LOGBACK_CONFIG_FILE}" ]; then
  FLUREE_LOGBACK_CONFIGURATION_FILE="${SYSTEM_CONFIG_DIR}/${SYSTEM_LOGBACK_CONFIG_FILE}"
fi

echo "Using logback config file ${FLUREE_LOGBACK_CONFIGURATION_FILE}"

JAVA_OPTS='-XX:+UseG1GC -XX:MaxGCPauseMillis=50'
JAVA_CMD_OPTS='-XX:+UseG1GC'

while [ $# -gt 0 ]; do
  case "$1" in
  -Xmx*)
    XMX=$1
    ;;
  -Xms*)
    XMS=$1
    ;;
  test)
    break
    ;;
  *)
    JAVA_OPTS="$JAVA_OPTS $1"
    ;;
  esac
  shift
done

if [ "$XMX" == "" ]; then
  XMX=-Xmx2g
fi
if [ "$XMS" == "" ]; then
  XMS=-Xms1g
fi

## first check if issuing a command (string that starts with ':' as the first arg)
if [ "${first_arg:0:1}" = : ]; then
  echo "Executing command: $first_arg"
  exec $JAVA_X ${XMX} ${XMS} ${JAVA_CMD_OPTS} -Dfdb.command=$first_arg \
     ${FLUREE_ARGS} -Dfdb.properties.file=${FLUREE_PROPERTIES} -Dfdb.log.ansi \
    -Dlogback.configurationFile=${FLUREE_LOGBACK_CONFIGURATION_FILE} \
    -jar $FLUREE_SERVER
  exit 0
else
  case "$first_arg" in
  *.properties)
    FLUREE_PROPERTIES=$first_arg
    shift
    ;;
  esac
fi

if [ "$FLUREE_PROPERTIES" == "" ]; then
  echo "No properties file specified. Using default properties file $DEFAULT_PROPERTIES_FILE."
  FLUREE_PROPERTIES="${THIS_DIR}/${DEFAULT_PROPERTIES_FILE}"
fi

if ! [ -f ${FLUREE_PROPERTIES} ]; then
  echo "Properties file ${FLUREE_PROPERTIES} does not exist. Exiting."
  exit 1
fi

if ! [ -f $FLUREE_SERVER ]; then
  echo "Fluree ledger JAR file not found. Looking for ${FLUREE_SERVER}. Exiting."
  exit 1
fi

# This needs to stay down here so that all of the checks above run first.
# Basically the purpose of this is get right up to the point of starting Fluree
# and then exit successfully iff we got that far.
if [ "$first_arg" = "test" ]; then
  echo "Fluree successfully installed and ready to run"
  exit 0
fi

FDB_LOG_FORMAT=${FDB_LOG_FORMAT:-ansi}
FLUREE_DB_LOG_LEVEL=${FLUREE_DB_LOG_LEVEL:-INFO}
FLUREE_RAFT_LOG_LEVEL=${FLUREE_RAFT_LOG_LEVEL:-INFO}

echo "Fluree ledger starting with properties file: ${FLUREE_PROPERTIES}"
echo "Fluree ledger starting with java options: ${XMS} ${XMX} ${JAVA_OPTS}"
echo "Log format is ${FDB_LOG_FORMAT}"
echo "fluree.db log level is ${FLUREE_DB_LOG_LEVEL}"
echo "fluree.raft log level is ${FLUREE_RAFT_LOG_LEVEL}"

exec ${JAVA_X} -server ${XMX} ${XMS} ${JAVA_OPTS} ${FLUREE_ARGS} -Dfdb.properties.file=${FLUREE_PROPERTIES} \
  -Dfdb.log.${FDB_LOG_FORMAT} -Dfluree.db.log.level=${FLUREE_DB_LOG_LEVEL} \
  -Dfluree.raft.log.level=${FLUREE_RAFT_LOG_LEVEL} -Dlogback.configurationFile=${FLUREE_LOGBACK_CONFIGURATION_FILE} \
  -jar ${FLUREE_SERVER}
