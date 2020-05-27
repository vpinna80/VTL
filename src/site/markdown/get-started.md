## SDMX Connectors for Statistical Software

The SDMX Connectors project has been developed with the aim of covering the 'last mile' in SDMX implementations. 

In particular, the focus of the project is to provide the **end user** a set of plugins that can be easily installed in the most popular data analysis tools (e.g. R, MATLAB, SAS, STATA, Excel, etc.) allowing a **direct access** to SDMX data from the tool.
***

As you can see from the architectural overview diagram below, all the plugins share a **common Java library**. The Java core contains most of the code, while the plugins are very simple façades that allow the seamless integration with the hosting tool.

![My image](https://github.com/amattioc/SDMX/blob/master/docs/resources/sdmx.png)

All the information for building, installing and using the SDMX Connectors can be found in the Wiki pages dedicated to the java core library or to the specific plugin.
