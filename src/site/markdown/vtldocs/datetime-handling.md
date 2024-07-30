# Handling Date and Time Period values

The VTL engine can read, process, and output any value from the Date and Time Period 
standard VTL domains. At the moment the Time Domain itself is instead not supported.

## Reading Dates from sources

The CSV environment and SDMX environment can read Date and Time Periods from the 
source data. For SDMX, the TIME_PERIOD time identifier will be a date or a time period
depending on the frequency of the time series. For CSV, the user can choose which of 
the two to use in the CSV header.

## Processing Date and Time Periods

VTL functions that operate on time series will recognize and process date and time 
period values as they are defined in the dataset metadata, according to each 
function specification.

Moreover, the cast operator can convert between date or time periods and string.
To perform this conversion, the cast operator accepts a third parameter that 
acts as a mask for conversion.

The mask accepts the following characters:
*  YYYY: A four-digits year
*  YYY: A three-digits year
*  YY: a two-digits year
*  M[onth]3: A three-letter english name for month (i.e. "Sep") 
*  M[onth]1: A single letter english name for month (i.e. "N") 
*  M[onth]: The english name for month (i.e. "August") 
*  D[ay]3: A three-letter english name for day (i.e. "Mon") 
*  D[ay]1: A single letter english name for month (i.e. "W")
*  D[ay]: The english name for month (i.e. "Thursday")
*  PPP: A three-digits day of year, zero padded 
*  MM: a two-digits month, zero padded
*  M: a one or two-digits month (i.e. 4, 12)
*  DD: a two-digits day of month, zero padded
*  D: a one or two-digits day of month (i.e. 7, 14, 21)
*  Any spaces, dashes or forward slashes will be put in the output.
*  Other characters will cause an error.

## Outputting date and time periods

Date and time periods will be output always in their canonical representation as
provided by the engine. If you want to output in a different format, you have to 
convert these values to string using a mask beforehand.