# Using the VTL Spark environment

## Loading data inside Apache Spark™

The Spark environment can create a dataset wrapping data that is available from your 
Spark installation. Available formats are CSV, plain text, parquet and json files.

Inside VTL, spark datasets can be referenced by using a common prefix. Since this requires
characters not allowed in VTL aliases, you must always surround the alias with single quotes.

For example the following aliases:

```
'spark:parquet:/home/myname/data.parquet'
'spark:csv:/home/myname/data.csv'
```

will reference two VTL datasets wrapping a Spark SQL DataFrame obtained from reading the input 
parquet and CSV files respectively.

## Setting component domains and roles

As for non-spark VTL CSV files, all spark sources must have properly formatted field names
to enable VTL to distinguish between components of different VTL domains or roles.

Please read the section on using CSV files for more information on how to format field names.

## Processing data inside Spark

For VTL unary expressions, such as negation, if the operand dataset was loaded from Spark or 
is the result of a Spark action, the VTL Spark environment will try to transform the VTL 
expression into Spark SQL actions and wrap the results into a new VTL dataset. 

The same happens for VTL binary expressions, when one of the operands is a scalar value. If 
instead both operands are datasets, and the left operand is a Spark dataset, the contents of 
the other dataset will be loaded and the computation will happen entirely within Spark.

In all other cases, the VTL engine will materialize all the operands and the computation 
will happen in-memory on the client machine. Note that this may cause delays if you are 
constantly moving data in and out of Spark.