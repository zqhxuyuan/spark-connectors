#!/bin/sh
# ----------------------------------------------------------------------------
#  Copyright 2001-2006 The Apache Software Foundation.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ----------------------------------------------------------------------------
#
#   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
#   reserved.


# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRGDIR=`dirname "$PRG"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`

# Reset the REPO variable. If you need to influence this use the environment setup file.
REPO=


# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true
           if [ -z "$JAVA_VERSION" ] ; then
             JAVA_VERSION="CurrentJDK"
           else
             echo "Using Java version: $JAVA_VERSION"
           fi
		   if [ -z "$JAVA_HOME" ]; then
		      if [ -x "/usr/libexec/java_home" ]; then
			      JAVA_HOME=`/usr/libexec/java_home`
			  else
			      JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JAVA_VERSION}/Home
			  fi
           fi       
           ;;
esac

if [ -z "$JAVA_HOME" ] ; then
  if [ -r /etc/gentoo-release ] ; then
    JAVA_HOME=`java-config --jre-home`
  fi
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java`
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly." 1>&2
  echo "  We cannot execute $JAVACMD" 1>&2
  exit 1
fi

if [ -z "$REPO" ]
then
  REPO="$BASEDIR"/repo
fi

CLASSPATH="$BASEDIR"/conf:"$REPO"/com/zqh/spark/connectors/filodb-core/1.0-SNAPSHOT/filodb-core-1.0-SNAPSHOT.jar:"$REPO"/io/kamon/kamon-core_2.11/0.6.0/kamon-core_2.11-0.6.0.jar:"$REPO"/org/hdrhistogram/HdrHistogram/2.1.7/HdrHistogram-2.1.7.jar:"$REPO"/org/slf4j/slf4j-api/1.7.7/slf4j-api-1.7.7.jar:"$REPO"/com/typesafe/scala-logging/scala-logging_2.11/3.5.0/scala-logging_2.11-3.5.0.jar:"$REPO"/org/scala-lang/scala-reflect/2.11.8/scala-reflect-2.11.8.jar:"$REPO"/com/beachape/enumeratum_2.11/1.2.1/enumeratum_2.11-1.2.1.jar:"$REPO"/com/beachape/enumeratum-macros_2.11/1.2.1/enumeratum-macros_2.11-1.2.1.jar:"$REPO"/org/velvia/filo/filo-scala_2.11/0.3.6/filo-scala_2.11-0.3.6.jar:"$REPO"/org/velvia/filo/schema_2.11/0.3.6/schema_2.11-0.3.6.jar:"$REPO"/org/velvia/filo/flatbuffers_2.11/0.2.0/flatbuffers_2.11-0.2.0.jar:"$REPO"/io/monix/monix-types_2.11/2.3.0/monix-types_2.11-2.3.0.jar:"$REPO"/io/monix/monix-execution_2.11/2.3.0/monix-execution_2.11-2.3.0.jar:"$REPO"/org/reactivestreams/reactive-streams/1.0.0/reactive-streams-1.0.0.jar:"$REPO"/io/monix/monix-eval_2.11/2.3.0/monix-eval_2.11-2.3.0.jar:"$REPO"/io/monix/monix-reactive_2.11/2.3.0/monix-reactive_2.11-2.3.0.jar:"$REPO"/org/jctools/jctools-core/2.0.1/jctools-core-2.0.1.jar:"$REPO"/joda-time/joda-time/2.2/joda-time-2.2.jar:"$REPO"/org/joda/joda-convert/1.2/joda-convert-1.2.jar:"$REPO"/com/googlecode/concurrentlinkedhashmap/concurrentlinkedhashmap-lru/1.4/concurrentlinkedhashmap-lru-1.4.jar:"$REPO"/net/ceedubs/ficus_2.11/1.1.2/ficus_2.11-1.1.2.jar:"$REPO"/org/scodec/scodec-bits_2.11/1.0.10/scodec-bits_2.11-1.0.10.jar:"$REPO"/io/fastjson/boon/0.33/boon-0.33.jar:"$REPO"/com/googlecode/javaewah/JavaEWAH/1.1.6/JavaEWAH-1.1.6.jar:"$REPO"/com/github/alexandrnikitin/bloom-filter_2.11/0.7.0/bloom-filter_2.11-0.7.0.jar:"$REPO"/com/github/rholder/fauxflake/fauxflake-core/1.1.0/fauxflake-core-1.1.0.jar:"$REPO"/org/scalactic/scalactic_2.11/2.2.6/scalactic_2.11-2.2.6.jar:"$REPO"/com/markatta/futiles_2.11/1.1.3/futiles_2.11-1.1.3.jar:"$REPO"/com/nativelibs4java/scalaxy-loops_2.11/0.3.3/scalaxy-loops_2.11-0.3.3.jar:"$REPO"/org/scala-lang/scala-compiler/2.11.2/scala-compiler-2.11.2.jar:"$REPO"/org/scala-lang/modules/scala-xml_2.11/1.0.2/scala-xml_2.11-1.0.2.jar:"$REPO"/com/nativelibs4java/scalaxy-streams_2.11/0.3.3/scalaxy-streams_2.11-0.3.3.jar:"$REPO"/com/zqh/spark/connectors/filodb-coordinator/1.0-SNAPSHOT/filodb-coordinator-1.0-SNAPSHOT.jar:"$REPO"/com/typesafe/config/1.3.1/config-1.3.1.jar:"$REPO"/com/typesafe/akka/akka-actor_2.11/2.3.15/akka-actor_2.11-2.3.15.jar:"$REPO"/com/typesafe/akka/akka-cluster_2.11/2.3.15/akka-cluster_2.11-2.3.15.jar:"$REPO"/com/typesafe/akka/akka-remote_2.11/2.3.15/akka-remote_2.11-2.3.15.jar:"$REPO"/io/netty/netty/3.8.0.Final/netty-3.8.0.Final.jar:"$REPO"/com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar:"$REPO"/org/uncommons/maths/uncommons-maths/1.2.2a/uncommons-maths-1.2.2a.jar:"$REPO"/com/typesafe/akka/akka-contrib_2.11/2.3.15/akka-contrib_2.11-2.3.15.jar:"$REPO"/com/typesafe/akka/akka-persistence-experimental_2.11/2.3.15/akka-persistence-experimental_2.11-2.3.15.jar:"$REPO"/org/iq80/leveldb/leveldb/0.5/leveldb-0.5.jar:"$REPO"/org/iq80/leveldb/leveldb-api/0.5/leveldb-api-0.5.jar:"$REPO"/org/fusesource/leveldbjni/leveldbjni-all/1.7/leveldbjni-all-1.7.jar:"$REPO"/org/fusesource/leveldbjni/leveldbjni/1.7/leveldbjni-1.7.jar:"$REPO"/org/fusesource/hawtjni/hawtjni-runtime/1.8/hawtjni-runtime-1.8.jar:"$REPO"/org/fusesource/leveldbjni/leveldbjni-osx/1.5/leveldbjni-osx-1.5.jar:"$REPO"/org/fusesource/leveldbjni/leveldbjni-linux32/1.5/leveldbjni-linux32-1.5.jar:"$REPO"/org/fusesource/leveldbjni/leveldbjni-linux64/1.5/leveldbjni-linux64-1.5.jar:"$REPO"/org/fusesource/leveldbjni/leveldbjni-win32/1.5/leveldbjni-win32-1.5.jar:"$REPO"/org/fusesource/leveldbjni/leveldbjni-win64/1.5/leveldbjni-win64-1.5.jar:"$REPO"/com/typesafe/akka/akka-slf4j_2.11/2.3.15/akka-slf4j_2.11-2.3.15.jar:"$REPO"/com/opencsv/opencsv/3.3/opencsv-3.3.jar:"$REPO"/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar:"$REPO"/net/jpountz/lz4/lz4/1.3.0/lz4-1.3.0.jar:"$REPO"/com/zqh/spark/connectors/filodb-cassandra/1.0-SNAPSHOT/filodb-cassandra-1.0-SNAPSHOT.jar:"$REPO"/com/datastax/cassandra/cassandra-driver-core/3.0.2/cassandra-driver-core-3.0.2.jar:"$REPO"/io/netty/netty-handler/4.0.33.Final/netty-handler-4.0.33.Final.jar:"$REPO"/io/netty/netty-buffer/4.0.33.Final/netty-buffer-4.0.33.Final.jar:"$REPO"/io/netty/netty-common/4.0.33.Final/netty-common-4.0.33.Final.jar:"$REPO"/io/netty/netty-transport/4.0.33.Final/netty-transport-4.0.33.Final.jar:"$REPO"/io/netty/netty-codec/4.0.33.Final/netty-codec-4.0.33.Final.jar:"$REPO"/com/google/guava/guava/16.0.1/guava-16.0.1.jar:"$REPO"/io/dropwizard/metrics/metrics-core/3.1.2/metrics-core-3.1.2.jar:"$REPO"/com/quantifind/sumac_2.11/0.3.0/sumac_2.11-0.3.0.jar:"$REPO"/org/scala-lang/scala-library/2.11.0/scala-library-2.11.0.jar:"$REPO"/org/scala-lang/modules/scala-parser-combinators_2.11/1.0.1/scala-parser-combinators_2.11-1.0.1.jar:"$REPO"/com/zqh/spark/connectors/filodb-cli/1.0-SNAPSHOT/filodb-cli-1.0-SNAPSHOT.jar

ENDORSED_DIR=
if [ -n "$ENDORSED_DIR" ] ; then
  CLASSPATH=$BASEDIR/$ENDORSED_DIR/*:$CLASSPATH
fi

if [ -n "$CLASSPATH_PREFIX" ] ; then
  CLASSPATH=$CLASSPATH_PREFIX:$CLASSPATH
fi

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --path --windows "$JAVA_HOME"`
  [ -n "$HOME" ] && HOME=`cygpath --path --windows "$HOME"`
  [ -n "$BASEDIR" ] && BASEDIR=`cygpath --path --windows "$BASEDIR"`
  [ -n "$REPO" ] && REPO=`cygpath --path --windows "$REPO"`
fi

exec "$JAVACMD" $JAVA_OPTS -Xms56m \
  -classpath "$CLASSPATH" \
  -Dapp.name="cli-main" \
  -Dapp.pid="$$" \
  -Dapp.repo="$REPO" \
  -Dapp.home="$BASEDIR" \
  -Dbasedir="$BASEDIR" \
  filodb.cli.CliMain \
  "$@"
