#!/usr/bin/env bash

set -e

if type -p java > /dev/null; then
    # found java executable in PATH
    JAVA_X=$(which java)
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    # echo found java executable in JAVA_HOME
    JAVA_X="$JAVA_HOME/bin/java"
else
    echo "Java is not installed or cannot be found. FlureeDB requires Java ${MINIMUM_JAVA_VERSION}+. If installed, check JAVA_HOME environment variable."
    exit 1
fi

echo "$JAVA_X"
