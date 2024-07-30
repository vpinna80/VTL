# Using CSV in VTL Engine

The VTL Engine allows you to use data contained in CSV files that you 

## Reading CSV data from VTL scripts

You can reference a CSV file in a VTL script by using a specially formatted identifier.
This file will be loaded as a standard VTL dataset. For example, in the following script:

```
d1 := 'csv:/user/john/my.csv'
```

d1 will be defined as an alias pointing to the actual CSV data in the file.

You can also directly reference a CSV file in expressions, without having to assign an alias to it first:

```
d1 := 'csv:/user/john/left.csv'#MEASURE + 'csv:/user/john/right.csv'#MEASURE 
```

In this case, d1 will contain the sum of column MEASURE for all the matching rows of the two CSV files.

## Formatting of CSV files for VTL

To be able to read CSV data from a text file, the VTL Engine requires for it to be
specially formatted in accordance to the engine parsing rules.

### Formatting values

The only allowed separator is the comma.

For numbers, the decimal separator is the dot, and there must not be any thousands 
separator. The scientific notation is supported only for the Number domain. 

For strings, they must be enclosed in double quotes only if they contain commas.
When quoting strings, "double double quotes" are replaced by a single double quotes 
character, and the enclosing double quotes are omitted from the value.

### Formatting headers

The header must be always the first row of the CSV file and it must be formatted 
according to the following rules.

Each column name in the header must be composed only of letters, digits and the underscore.

Each column name may preceded by exactly one of the following:
*  A dollar character, meaning the column is to be treated as an identifier;
*  A sharp character, meaning the column is to be treated as an attribute.
*  No character, meaning the column is to be treated as a measure.

Each column name must be followed by exacly one of the following:

*  =Number, meaning the column contains double precision floating point numbers;
*  =Integer, meaning the column contains signed integer numbers in the range from 
   -2<sup>63</sup> to 2<sup>63</sup>;
*  =String, meaning the column contains quoted or unquoted strings;
*  =Boolean, meaning the column contains the literals "True" or "False";
*  =Date[pattern], meaning the column contains dates formatted according to "pattern";
*  =Time_Period[pattern], meaning the column contains time periods formatted according to "pattern";
*  =cl_name, meaning the column contain codes from the codelist "cl_name" that must be 
   already defined in the metadata repository. 

Here's an example of a valid header for consumption by the VTL Engine:

```
$TIME_PERIOD=Date[YYYY-MM-DD],$REF_AREA=CL_COUNTRY,OBS_VALUE=Number,#OBS_STATUS=String
```
