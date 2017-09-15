@REM ----------------------------------------------------------------------------
@REM  Copyright 2001-2006 The Apache Software Foundation.
@REM
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM
@REM       http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.
@REM ----------------------------------------------------------------------------
@REM
@REM   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
@REM   reserved.

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup
set REPO=


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\repo

set CLASSPATH="%BASEDIR%"\conf;"%REPO%"\com\zqh\spark\connectors\filodb-core\1.0-SNAPSHOT\filodb-core-1.0-SNAPSHOT.jar;"%REPO%"\io\kamon\kamon-core_2.11\0.6.0\kamon-core_2.11-0.6.0.jar;"%REPO%"\org\hdrhistogram\HdrHistogram\2.1.7\HdrHistogram-2.1.7.jar;"%REPO%"\org\slf4j\slf4j-api\1.7.7\slf4j-api-1.7.7.jar;"%REPO%"\com\typesafe\scala-logging\scala-logging_2.11\3.5.0\scala-logging_2.11-3.5.0.jar;"%REPO%"\org\scala-lang\scala-reflect\2.11.8\scala-reflect-2.11.8.jar;"%REPO%"\com\beachape\enumeratum_2.11\1.2.1\enumeratum_2.11-1.2.1.jar;"%REPO%"\com\beachape\enumeratum-macros_2.11\1.2.1\enumeratum-macros_2.11-1.2.1.jar;"%REPO%"\org\velvia\filo\filo-scala_2.11\0.3.6\filo-scala_2.11-0.3.6.jar;"%REPO%"\org\velvia\filo\flatbuffers_2.11\0.2.0\flatbuffers_2.11-0.2.0.jar;"%REPO%"\org\velvia\filo\schema_2.11\0.3.6\schema_2.11-0.3.6.jar;"%REPO%"\io\monix\monix-types_2.11\2.3.0\monix-types_2.11-2.3.0.jar;"%REPO%"\io\monix\monix-execution_2.11\2.3.0\monix-execution_2.11-2.3.0.jar;"%REPO%"\org\reactivestreams\reactive-streams\1.0.0\reactive-streams-1.0.0.jar;"%REPO%"\io\monix\monix-eval_2.11\2.3.0\monix-eval_2.11-2.3.0.jar;"%REPO%"\io\monix\monix-reactive_2.11\2.3.0\monix-reactive_2.11-2.3.0.jar;"%REPO%"\org\jctools\jctools-core\2.0.1\jctools-core-2.0.1.jar;"%REPO%"\joda-time\joda-time\2.2\joda-time-2.2.jar;"%REPO%"\org\joda\joda-convert\1.2\joda-convert-1.2.jar;"%REPO%"\com\googlecode\concurrentlinkedhashmap\concurrentlinkedhashmap-lru\1.4\concurrentlinkedhashmap-lru-1.4.jar;"%REPO%"\net\ceedubs\ficus_2.11\1.1.2\ficus_2.11-1.1.2.jar;"%REPO%"\org\scodec\scodec-bits_2.11\1.0.10\scodec-bits_2.11-1.0.10.jar;"%REPO%"\io\fastjson\boon\0.33\boon-0.33.jar;"%REPO%"\com\googlecode\javaewah\JavaEWAH\1.1.6\JavaEWAH-1.1.6.jar;"%REPO%"\com\github\alexandrnikitin\bloom-filter_2.11\0.7.0\bloom-filter_2.11-0.7.0.jar;"%REPO%"\com\github\rholder\fauxflake\fauxflake-core\1.1.0\fauxflake-core-1.1.0.jar;"%REPO%"\org\scalactic\scalactic_2.11\2.2.6\scalactic_2.11-2.2.6.jar;"%REPO%"\com\markatta\futiles_2.11\1.1.3\futiles_2.11-1.1.3.jar;"%REPO%"\com\nativelibs4java\scalaxy-loops_2.11\0.3.3\scalaxy-loops_2.11-0.3.3.jar;"%REPO%"\org\scala-lang\scala-compiler\2.11.2\scala-compiler-2.11.2.jar;"%REPO%"\org\scala-lang\modules\scala-xml_2.11\1.0.2\scala-xml_2.11-1.0.2.jar;"%REPO%"\com\nativelibs4java\scalaxy-streams_2.11\0.3.3\scalaxy-streams_2.11-0.3.3.jar;"%REPO%"\net\jpountz\lz4\lz4\1.3.0\lz4-1.3.0.jar;"%REPO%"\com\zqh\spark\connectors\filodb-coordinator\1.0-SNAPSHOT\filodb-coordinator-1.0-SNAPSHOT.jar;"%REPO%"\com\typesafe\config\1.3.1\config-1.3.1.jar;"%REPO%"\com\typesafe\akka\akka-actor_2.11\2.3.15\akka-actor_2.11-2.3.15.jar;"%REPO%"\com\typesafe\akka\akka-cluster_2.11\2.3.15\akka-cluster_2.11-2.3.15.jar;"%REPO%"\com\typesafe\akka\akka-remote_2.11\2.3.15\akka-remote_2.11-2.3.15.jar;"%REPO%"\io\netty\netty\3.8.0.Final\netty-3.8.0.Final.jar;"%REPO%"\com\google\protobuf\protobuf-java\2.5.0\protobuf-java-2.5.0.jar;"%REPO%"\org\uncommons\maths\uncommons-maths\1.2.2a\uncommons-maths-1.2.2a.jar;"%REPO%"\com\typesafe\akka\akka-contrib_2.11\2.3.15\akka-contrib_2.11-2.3.15.jar;"%REPO%"\com\typesafe\akka\akka-persistence-experimental_2.11\2.3.15\akka-persistence-experimental_2.11-2.3.15.jar;"%REPO%"\org\iq80\leveldb\leveldb\0.5\leveldb-0.5.jar;"%REPO%"\org\iq80\leveldb\leveldb-api\0.5\leveldb-api-0.5.jar;"%REPO%"\org\fusesource\leveldbjni\leveldbjni-all\1.7\leveldbjni-all-1.7.jar;"%REPO%"\org\fusesource\leveldbjni\leveldbjni\1.7\leveldbjni-1.7.jar;"%REPO%"\org\fusesource\hawtjni\hawtjni-runtime\1.8\hawtjni-runtime-1.8.jar;"%REPO%"\org\fusesource\leveldbjni\leveldbjni-osx\1.5\leveldbjni-osx-1.5.jar;"%REPO%"\org\fusesource\leveldbjni\leveldbjni-linux32\1.5\leveldbjni-linux32-1.5.jar;"%REPO%"\org\fusesource\leveldbjni\leveldbjni-linux64\1.5\leveldbjni-linux64-1.5.jar;"%REPO%"\org\fusesource\leveldbjni\leveldbjni-win32\1.5\leveldbjni-win32-1.5.jar;"%REPO%"\org\fusesource\leveldbjni\leveldbjni-win64\1.5\leveldbjni-win64-1.5.jar;"%REPO%"\com\typesafe\akka\akka-slf4j_2.11\2.3.15\akka-slf4j_2.11-2.3.15.jar;"%REPO%"\com\opencsv\opencsv\3.3\opencsv-3.3.jar;"%REPO%"\org\apache\commons\commons-lang3\3.3.2\commons-lang3-3.3.2.jar;"%REPO%"\com\zqh\spark\connectors\filodb-cassandra\1.0-SNAPSHOT\filodb-cassandra-1.0-SNAPSHOT.jar;"%REPO%"\com\datastax\cassandra\cassandra-driver-core\3.0.2\cassandra-driver-core-3.0.2.jar;"%REPO%"\io\netty\netty-handler\4.0.33.Final\netty-handler-4.0.33.Final.jar;"%REPO%"\io\netty\netty-buffer\4.0.33.Final\netty-buffer-4.0.33.Final.jar;"%REPO%"\io\netty\netty-common\4.0.33.Final\netty-common-4.0.33.Final.jar;"%REPO%"\io\netty\netty-transport\4.0.33.Final\netty-transport-4.0.33.Final.jar;"%REPO%"\io\netty\netty-codec\4.0.33.Final\netty-codec-4.0.33.Final.jar;"%REPO%"\com\google\guava\guava\16.0.1\guava-16.0.1.jar;"%REPO%"\io\dropwizard\metrics\metrics-core\3.1.2\metrics-core-3.1.2.jar;"%REPO%"\com\quantifind\sumac_2.11\0.3.0\sumac_2.11-0.3.0.jar;"%REPO%"\org\scala-lang\scala-library\2.11.0\scala-library-2.11.0.jar;"%REPO%"\org\scala-lang\modules\scala-parser-combinators_2.11\1.0.1\scala-parser-combinators_2.11-1.0.1.jar;"%REPO%"\com\zqh\spark\connectors\filodb-cli\1.0-SNAPSHOT\filodb-cli-1.0-SNAPSHOT.jar

set ENDORSED_DIR=
if NOT "%ENDORSED_DIR%" == "" set CLASSPATH="%BASEDIR%"\%ENDORSED_DIR%\*;%CLASSPATH%

if NOT "%CLASSPATH_PREFIX%" == "" set CLASSPATH=%CLASSPATH_PREFIX%;%CLASSPATH%

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS% -Xms56m -classpath %CLASSPATH% -Dapp.name="cli-main" -Dapp.repo="%REPO%" -Dapp.home="%BASEDIR%" -Dbasedir="%BASEDIR%" filodb.cli.CliMain %CMD_LINE_ARGS%
if %ERRORLEVEL% NEQ 0 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=%ERRORLEVEL%

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@REM If error code is set to 1 then the endlocal was done already in :error.
if %ERROR_CODE% EQU 0 @endlocal


:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%
