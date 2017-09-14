https://github.com/SnappyDataInc/snappy-poc

SnappData dependency:

snappydata-core -> snappydata-store-tools

snappydata-cluster -> snappydata-spark

1. remote locator, Exception:

SnappyTSocket: Failed to create or configure socket.

2. cannot init gemfilestore

Solve: add snappydata-store-tools dependency

https://mvnrepository.com/artifact/io.snappydata/snappydata-core_2.11/0.9

3. version mismatch

NoSuchMethodError: org.apache.spark.sql.catalyst.plans.physical.OrderlessHashPartitioning

Solve: check snappydata-cluster and snapp-spark version. for ex

https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11/0.9

4. kafka not start:

17/09/14 16:11:39 INFO SimpleConsumer: Reconnect due to socket error: java.nio.channels.ClosedChannelException
Exception in thread "main" org.apache.spark.SparkException: java.nio.channels.ClosedChannelException

Solve: use kafka_2.11-0.8.2.2

5. connect success:

```
17/09/14 16:15:09 INFO HiveClientUtil: Using SnappyStore as metastore database,dbURL = jdbc:snappydata://192.168.6.52:1527/;route-query=false;
17/09/14 16:15:46 INFO SnappyStreamingContext: StreamingContext started
17/09/14 16:15:47 INFO JobScheduler: Added jobs for time 1505376947000 ms
17/09/14 16:15:47 INFO JobScheduler: Starting job streaming job 1505376947000 ms.0 from job set of time 1505376947000 ms
17/09/14 16:15:48 INFO FilteredDStream: Slicing from 1505376948000 ms to 1505376948000 ms (aligned to 1505376948000 ms and 1505376948000 ms)
```

result:

```
+-----------+---+--------------------+-------------------+----+-------+
|  publisher|geo|           timestamp|            avg_bid|imps|uniques|
+-----------+---+--------------------+-------------------+----+-------+
|publisher25| TX|2017-09-14 16:30:...|0.04485066386508268|   1|      1|
|publisher25| MT|2017-09-14 16:30:...| 0.6691683623802358|   1|      1|
|publisher23| SC|2017-09-14 16:30:...| 0.9165101078211507|   1|      1|
|publisher21| MN|2017-09-14 16:30:...|  0.969299255071159|   1|      1|
|publisher26| MS|2017-09-14 16:30:...|0.24663119944219614|   1|      1|
|publisher36| NJ|2017-09-14 16:30:...|0.12422401448156745|   1|      1|
|publisher23| MS|2017-09-14 16:30:...|0.30382419158465307|   1|      1|
```
