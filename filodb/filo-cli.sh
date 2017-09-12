#!/usr/bin/env bash
CLI_FILE=`pwd`"/cli/bin/cli-main.sh"

if [ ! -f "$CLI_FILE" ];then
    mvn package -Dmaven.test.skip=true
    $CLI_FILE --command init
fi

$CLI_FILE "$@"
