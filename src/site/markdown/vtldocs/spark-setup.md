# Setting up the VTL Spark environment

## Download and install Apache Spark™

Refer to the product site to download and install Apache Spark™ on your machine.

You don't need to download or install it if an installation folder is already accessible.

Usually, a Spark installation is self-contained and does not require additional libraries 
to be installed in the client system. 

## `SPARK_HOME` environment variable and classpath 

In order to use the VTL Spark environment from VTL Studio, you need to set the environment 
variable SPARK_HOME to the absolute path of the Spark installation.

If the variable is not set system.wide, you may do that using `Sys.setenv()` directly 
from the R prompt, before starting VTL Studio.

In order to use the VTL Spark environment outside VTL Studio, for example from the CLI or
the REST web services, you need to instead add the needed Spark jars to the classpath.

To set the classpath, please refer to OpenJDK or the servlet-compliant server documentation.

## Enabling the environment in VTL Studio

If you have followed the setup instructions, when loading the RVTL package,
the Spark environment will be loaded as disabled.

In VTL Studio settings tab, drag "Spark Environment" from the "Available Environments"
section to the "Enabled Environments" section.

In the environment properties, select "Spark Environment" from the dropdown list, 
then, in the "Select property" dropdown, select the MASTER property, and set the master 
URL. 

If you don't know the URL, please contact your IT representative. For testing purposes,
you may also set "local" as the master url, in which case Spark will be launched from inside 
R. Note that this approach may cause high memory consumption on your computer. 

The Spark environment will remain active until you disable it or quit R.   