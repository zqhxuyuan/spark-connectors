
# FiloDB From TupleJump(Apple)

https://github.com/filodb/FiloDB

## Overview

filodb main module include: core/coordinator/cassandra.
cli and spark both depends on this three main modules.
cli and spark is the one of two way to running filodb.
You can run on cli shell script, or by spark-shell/submit.


## Changes:

1. stb to maven
2. appassembler-maven-plugin build cli script
3. share core module's test by test-jar

## Build:

mvn package -Dmaven.test.skip=true

After package all modules, you can checkout cli and spark module's target.

core/filodb-cli-jar-with-dependencies.jar used in cli shell mode.
spark/filodb-spark-jar-with-dependencies.jar used in spark mode.

## Running:

- https://github.com/filodb/FiloDB#cli-example

1. start cassandra
2. ./filo-cli --command create --dataset gdelt --columns GLOBALEVENTID:int,SQLDATE:string,MonthYear:int,Year:int,FractionDate:double,Actor1Code:string,Actor1Name:string,Actor1CountryCode:string,Actor1KnownGroupCode:string,Actor1EthnicCode:string,Actor1Religion1Code:string,Actor1Religion2Code:string,Actor1Type1Code:string,Actor1Type2Code:string,Actor1Type3Code:string,Actor2Code:string,Actor2Name:string,Actor2CountryCode:string,Actor2KnownGroupCode:string,Actor2EthnicCode:string,Actor2Religion1Code:string,Actor2Religion2Code:string,Actor2Type1Code:string,Actor2Type2Code:string,Actor2Type3Code:string,IsRootEvent:int,EventCode:string,EventBaseCode:string,EventRootCode:string,QuadClass:int,GoldsteinScale:double,NumMentions:int,NumSources:int,NumArticles:int,AvgTone:double,Actor1Geo_Type:int,Actor1Geo_FullName:string,Actor1Geo_CountryCode:string,Actor1Geo_ADM1Code:string,Actor1Geo_Lat:double,Actor1Geo_Long:double,Actor1Geo_FeatureID:int,Actor2Geo_Type:int,Actor2Geo_FullName:string,Actor2Geo_CountryCode:string,Actor2Geo_ADM1Code:string,Actor2Geo_Lat:double,Actor2Geo_Long:double,Actor2Geo_FeatureID:int,ActionGeo_Type:int,ActionGeo_FullName:string,ActionGeo_CountryCode:string,ActionGeo_ADM1Code:string,ActionGeo_Lat:double,ActionGeo_Long:double,ActionGeo_FeatureID:int,DATEADDED:string,Actor1Geo_FullLocation:string,Actor2Geo_FullLocation:string,ActionGeo_FullLocation:string --rowKeys GLOBALEVENTID
3. ./filo-cli --command list --dataset gdelt
4. ./filo-cli --command importcsv --dataset gdelt --filename GDELT-1979-1984-100000.csv
5. ./filo-cli --dataset gdelt --select MonthYear,Actor2Code --limit 5 --outfile out.csv

Notice: Step 4 may not work sometimes, just try again

```
# Failure Log:
Error Ingestion actors shut down from ref Actor[akka://filo-cli/user/coordinator#1316914830],
check error logs setting up CSV ingestion of gdelt/0 at GDELT-1979-1984-100000.csv

# Success Log:
Ingestion set up for gdelt / 0, starting...
Ingestion of GDELT-1979-1984-100000.csv finished!
```

Question: How to add logback.xml?
Answer: add below config to cli-main.sh

```
-Dlogback.configurationFile="$BASEDIR/conf/logback.xml"
```