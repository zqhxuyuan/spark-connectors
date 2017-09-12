#!/usr/bin/env bash
CLI_FILE=`pwd`"/cli/bin/cli-main.sh"
if [ ! -f "$CLI_FILE" ];then
    mvn package -Dmaven.test.skip=true
    $CLI_FILE --command init
fi
$CLI_FILE "$@"

#CLI_JAR=`pwd`"/cli/target/filodb-cli-jar-with-dependencies.jar"
#CORE_JAR=`pwd`"/core/target/filodb-core-jar-with-dependencies.jar"
#COOR_JAR=`pwd`"/coordinator/target/filodb-coordinator-jar-with-dependencies.jar"
#CASS_JAR=`pwd`"/cassandra/target/filodb-cassandra-jar-with-dependencies.jar"

#exec $JAVA_HOME/bin/java -Xms56m \
#  -classpath "$CLI_JAR:$CORE_JAR:$COOR_JAR:$CASS_JAR" \
#  filodb.cli.CliMain \
#  "$@"
