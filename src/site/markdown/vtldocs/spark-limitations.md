# Limitations of the Spark VTL environment

The VTL Spark environment is still under development. As it is, it has several limitations
to take into account when moving VTL computations inside Apache Spark™.

## Time series and time functions

The Spark environment doesn't support any time-series manipulation inside Spark,
as Spark does not provide an out-of-the-box API for manipulating time series.

When invoking VTL time functions, such as `fill_time_series()`, the dataset will be 
materialized in the client machine and the computation will proceed outside Spark.

## Dates

The VTL Spark environment will map VTL dates using classes in the `java.time` package.
Source data, such as parquet files, that contains serialized instances of the old 
`java.sql.Date` and `java.sql.Timestamp` classes may not be loaded directly. 

Instead, those files must be translated first, converting all instances of the old classes 
into instances of the new classes in the `java.time` package. Note that this is supported
only from Apache Spark™ 3.0.0 onwards.

Please read about `spark.sql.datetime.java8API.enabled` configuration parameter on
official [Apache Spark™ SQL documentation](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration). 

## Time periods, Durations and Times

The VTL time period, duration and time domains have not been translated to work inside Spark 
yet. Any dataset containing a component of those domains will cause the engine to crash.

Please use VTL `cast` function to convert those components into some other supported domain.

## Codelists (enumerated subsets of string domain)

Code lists may cause some issues when stored in a Spark dataset. Either they will be
converted back to simple strings, or cause some other unexpected behaviour.

## Parallel computations

When processing VTL expressions inside Spark, you may want to disable parallelism, as
Spark will parallelize the computation by itself, to reduce the memory consumption 
and number of Java threads that will be created.

You can set the `vtl.sequential` java system property to `true` to disable parallelism.
