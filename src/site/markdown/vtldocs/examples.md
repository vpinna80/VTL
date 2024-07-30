# VTL Examples

Those examples show how to use pre-existing data and some simple manipulations.

After creating a session, paste the code snippets into the editor and use the Dataset Explorer to browse the results.

## Arithmetics on scalar values 

This example shows how to perform simple arithmetics manipulations on scalar data:

```
a := 0.1;
b := 3;
c := abs(sqrt(14 + a));
d := a + b + c;
e := c + d / a;
f := e - d;
g := -f;
test := a * b + c / a;
```

## Pulling data from R

This example shows how to manipulate into the VTL engine an R data frame that is defined in the R global environment.

### R code

First prepare the data inside your R session:

```R
# retrieve gbp and usd rates from the ECB SDMX web service
rates <- getTimeSeriesTable('ECB', 'EXR.A.USD+GBP.EUR.SP00.A')
# set needed metadata for VTL engine (identifiers and measures)
attr(rates, 'measures') <- 'OBS_VALUE'
attr(rates, 'identifiers') <- c('TIME_PERIOD', 'CURRENCY')
```

Note that if you access internet through a proxy server, you may need to configure it to replicate this example.

### VTL code

After your data is prepared, you may start vtlStudio, create a new session, and run the example:

```
/* create dataset for USD rate */
usd := rates[filter CURRENCY="USD"];
/* create dataset for GBP rate */
gbp := rates[filter CURRENCY="GBP"];
/* filter on time period */ 
usd_post_2000 := usd[filter TIME_PERIOD >  "2000"];
```

You may view the resulting `usd_post_2000` dataset in the explorer.

## Pulling data from web services

This example shows how to leverage the SDMX Connectors to directly pull data from a SDMX 2.1 compliant web service.

In the snippet the ECB SDMX web service is used as an example.

Note that if you access internet through a proxy server, you may need to configure it to replicate this example.

```
/* Retrieve food and beverages monthly component value */
compOfBasket := 'ECB:ICP(1.0)/M.U2.N.010000.4.ANR'[keep OBS_VALUE]
	[calc identifier YEAR := cast(time_agg("A", TIME_PERIOD), time, "")];

/* Retrieve annual component weight */
compWeight := 'ECB:ICP(1.0)/A.U2.N.010000.4.INW'
	[keep OBS_VALUE]
	[rename OBS_VALUE to WEIGHT, TIME_PERIOD to YEAR];

/* Join to calculate monthly weight */
monthlyWeight := inner_join(compWeight, compOfBasket)
	[keep WEIGHT]
	[rename WEIGHT to OBS_VALUE] / 1000;

/* Calculate component contribution */
calculated := compOfBasket * monthlyWeight;

/* Retrieve contribution from the provider */
expected := 'ECB:ICP(1.0)/M.U2.N.010000.3.CTG'
	[keep OBS_VALUE]
	[rename OBS_VALUE to expected_value];

/* Join to compare and calculate percentual error for easy comparison */
errors := inner_join(calculated, expected)
	[calc errorPerc := abs((number_var - expected_value) / expected_value) * 100];
```

## Loading CSV data from file system

This example shows how to load data from a CSV file with a modified header that complies with a predefined structure.

Example files are available in the package installation directory: `<package_root>/vtlStudio2/test_data`.

```
/* Load csv */ 
usd := 'csv:<package_root>/vtlStudio2/test_data/ecbexrusd_vtl.csv';

/* Filter on time period */ 
usd_post_2000 := usd[filter TIME_PERIOD >  "2000"];
```
