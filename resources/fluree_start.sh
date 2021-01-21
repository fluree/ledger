#!/usr/bin/env bash

set -e

THIS_DIR="$(dirname "$0")"
SYSTEM_JAR_DIR=/usr/local/share/java
SYSTEM_CONFIG_DIR=/usr/local/etc

FLUREE_LEDGER_JAR=fluree-ledger.standalone.jar
DEFAULT_PROPERTIES_FILE=fluree_sample.properties
PROPERTIES_FILE=fluree.properties

DEFAULT_LOGBACK_CONFIG_FILE=logback.xml
SYSTEM_LOGBACK_CONFIG_FILE=fluree-logback.xml

FLUREE_SERVER=""
FLUREE_PROPERTIES=""
FLUREE_LOGBACK_CONFIGURATION_FILE=""

## Find Java executable
JAVA_X=$(./find_java.sh)

## Check Java Version
./java_version.sh "$JAVA_X"

## decide if we're using local JAR or system-wide JAR
if [ -f ${THIS_DIR}/${FLUREE_LEDGER_JAR} ]; then
  FLUREE_SERVER="${THIS_DIR}/${FLUREE_LEDGER_JAR}"
else
  FLUREE_SERVER="${SYSTEM_JAR_DIR}/${FLUREE_LEDGER_JAR}"
fi

## decide if we're using local properties file or system-wide
if [ -f "${THIS_DIR}/${PROPERTIES_FILE}" ]; then
  FLUREE_PROPERTIES="${THIS_DIR}/${PROPERTIES_FILE}"
elif [ -f "${SYSTEM_CONFIG_DIR}/${PROPERTIES_FILE}" ]; then
  FLUREE_PROPERTIES="${SYSTEM_CONFIG_DIR}/${PROPERTIES_FILE}"
fi

## decide if we're using local logback config file or system-wide
if [ -f "${THIS_DIR}/${DEFAULT_LOGBACK_CONFIG_FILE}" ]; then
  FLUREE_LOGBACK_CONFIGURATION_FILE="${THIS_DIR}/${DEFAULT_LOGBACK_CONFIG_FILE}"
elif [ -f "${SYSTEM_CONFIG_DIR}/${SYSTEM_LOGBACK_CONFIG_FILE}" ]; then
  FLUREE_LOGBACK_CONFIGURATION_FILE="${SYSTEM_CONFIG_DIR}/${SYSTEM_LOGBACK_CONFIG_FILE}"
fi

## first check if issuing a command (string that starts with ':' as the only arg)
if [ "${1:0:1}" = : ]; then
    echo "Executing command: $1"
    exec $JAVA_X -Dfdb.command=$1 -jar $FLUREE_SERVER
    exit 0
else
    case "$1" in
        *.properties)
       FLUREE_PROPERTIES=$1
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

JAVA_OPTS='-XX:+UseG1GC -XX:MaxGCPauseMillis=50'

while [ $# -gt 0 ]
do
    case "$1" in
        -Xmx*)
            XMX=$1
            ;;
        -Xms*)
            XMS=$1
            ;;
        *)
            JAVA_OPTS="$JAVA_OPTS $1"
            ;;
    esac
    shift
done

if [ "$XMX" == "" ]; then
    XMX=-Xmx1g
fi
if [ "$XMS" == "" ]; then
    XMS=-Xms1g
fi

if ! [ -f $FLUREE_SERVER ]; then
    echo "Fluree ledger JAR file not found. Looking for ${FLUREE_SERVER}. Exiting."
    exit 1
fi

echo "Fluree ledger starting with properties file: ${FLUREE_PROPERTIES}"
echo "Fluree ledger starting with java options: ${XMS} ${XMX} ${JAVA_OPTS}"

exec ${JAVA_X} -server ${XMX} ${XMS} ${JAVA_OPTS} ${FLUREE_ARGS} -Dfdb.properties.file=${FLUREE_PROPERTIES} \
     -Dfdb.log.ansi -Dlogback.configurationFile=${FLUREE_LOGBACK_CONFIGURATION_FILE} -jar ${FLUREE_SERVER}
