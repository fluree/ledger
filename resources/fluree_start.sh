#!/usr/bin/env bash

set -e

MYDIR="$(dirname "$0")"

cd -- $MYDIR

FLUREE_SERVER=fluree-ledger.standalone.jar
DEFAULT_PROPERTIES_FILE=fluree_sample.properties

## Check Java Version
if type -p java; then
    # echo found java executable in PATH
    JAVA_X=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    # echo found java executable in JAVA_HOME
    JAVA_X="$JAVA_HOME/bin/java"
else
    echo "Java is not installed or cannot be found. FlureeDB requires Java 1.8. If installed, check JAVA_X_HOME environment."
    exit 1
fi

if [[ "$JAVA_X" ]]; then
    JAVA_VERSION=$("$JAVA_X" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    if [[ "$JAVA_VERSION" == *"."* ]] && (( "$JAVA_VERSION" < "1.8" )); then
        echo "FlureeDB requires minimum Java 8 (v1.8). Your version is: $JAVA_VERSION. Exiting."
        exit 1
    elif [[ "$JAVA_VERSION" != *"."* ]] && (( "$JAVA_VERSION" < "8" )); then
        echo "FlureeDB requires minimum Java 8 (v1.8). Your version is: $JAVA_VERSION. Exiting."
        exit 1
    else
        echo "Java version $JAVA_VERSION."
    fi
fi

## first check if issuing a command (string that starts with ':' as the only arg)
if [ "${1:0:1}" = : ]; then
    echo "Executing command: $1"
    exec $JAVA_X -Dfdb.command=$1 -jar $FLUREE_SERVER
    exit 0
else
    case "$1" in
        *.properties)
       PROPERTIES_FILE=$1
       shift
       ;;
    esac
fi

if [ "$PROPERTIES_FILE" == "" ]; then
    echo "No properties file specified. Using default properties file $DEFAULT_PROPERTIES_FILE."
    PROPERTIES_FILE=$DEFAULT_PROPERTIES_FILE
fi


if ! [ -f $PROPERTIES_FILE ]; then
    echo "Properties file $PROPERTIES_FILE does not exist. Exiting."
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
    echo "Fluree ledger JAR file not found. Looking for $FLUREE_SERVER. Exiting."
    exit 1
fi

echo "Fluree ledger starting with properties file: $PROPERTIES_FILE"
echo "Fluree ledger starting with java options: $XMS $XMX $JAVA_OPTS"

exec $JAVA_X -server $XMX $XMS $JAVA_OPTS $FLUREE_ARGS -Dfdb.properties.file=$PROPERTIES_FILE -Dfdb.log.ansi -Dlogback.configurationFile=./logback.xml -jar $FLUREE_SERVER
