# TODO
# R Environments
Among the numerous Environments supported by VTL Engine & Studio features the R environment to seamlessy get data from R to your VTL program.
To enable the R environment open the "Engine Settings" Tab in the VTL Studio editor and drag "R Environment" inside the active Environment list.
## How to
when vtlStudio is launched from your R session, it loads all dataframe with the attributes "measures" and "identifiers", the former containing the name of the column in the dataframe containing the measure of the VTL data, and the latter containing a list of the column in the dataframe to use as unique identifiers.
Then the dataframe in R will be visible from VTL Studio and can be used in your vtl Program.
