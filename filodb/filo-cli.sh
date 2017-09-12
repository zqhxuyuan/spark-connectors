#!/usr/bin/env bash

# set -x
SCALA_VERSION="2.11"
CLI_FILE=`pwd`"/cli/target/filo-cli.jar"

if [ ! -f "$CLI_FILE" ];then
    mvn package -Dmaven.test.skip=true
    $CLI_FILE --command init
fi

$CLI_FILE "$@"
