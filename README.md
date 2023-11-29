[![License](https://img.shields.io/badge/license-EUPL-green)](https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12)
![VTL Engine builds](https://github.com/vpinna80/VTL/workflows/VTL%20Engine%20builds/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/c20a3a19b6744db191d9dd1b1b3a8cbf)](https://www.codacy.com/manual/valentino.pinna/VTL?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=vpinna80/VTL&amp;utm_campaign=Badge_Grade)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/vpinna80/VTL?label=github-release)
![Maven metadata URL](https://img.shields.io/maven-metadata/v?label=maven-release&metadataUrl=https%3A%2F%2Frepo1.maven.org%2Fmaven2%2Fit%2Fbancaditalia%2Foss%2Fvtl%2Fvtl%2Fmaven-metadata.xml)
![CRAN/METACRAN](https://img.shields.io/cran/v/RVTL?label=cran-release)

# VTL E&E

An engine and editor for the 
[Validation and Transformation Language](https://sdmx.org/?page_id=5096), 
written in Java, Javascript and R.

## Usage and documentation

Usage info, documentation and examples are available at the
[project site](http://vpinna80.github.io/VTL/).

Now you can try the VTLStudio live on ShinyApps: [VTLStudio](https://vpinna80.shinyapps.io/vtlStudio/).

## Contributing to the project

All contributions are welcome!

Please take a moment to read the [Guideline](CONTRIBUTING.md).

## Build information

A complete description of the project modules and the build process will be available soon.

To build this project, you will need:

* A Windows or Linux machine;

> Building on a MacOS machine should be possible by now but **it is untested**.

* JDK >= 11;
* [Apache Maven](https://maven.apache.org/) 3.9.4;
* [GNU R](https://www.r-project.org/) >= 4.3.2 for building RVTL package;
* Configured internet connection (to download [node.js](https://nodejs.org/)).

To build the project, launch the command:

    mvn [-P with-r,with-sdmx,with-spark,with-jupyter,with-cli,with-rest] clean package

Each artifact will be generated inside the `target` folder of each module.
The optional maven profiles allow you to build any of the provided VTL bundles for
the different front-ends capable of communicating with the VTL engine.

If you want to build with support for the Spark evironment, activate the 
`with-spark` maven profile. Each bundle created with this build will contain the 
Spark environment.

If you want to build with support for SDMX web services, activate the
`with-sdmx` maven profile. Each bundle created with this build will contain the support
for interacting with a SDMX registry. Note that this requires that the 
[sdmx-core](https://github.com/bis-med-it/sdmx-core) maven artifacts are already installed 
in the local repository, because the artifacts are distributed only in source form 
by the Bank of International Settlements.

If you want to build the editor and the R package along with the engine, activate the 
`with-r` maven profile. You may need to configure your internet connection in 
Maven settings; you also need to install the REngine.jar artifact into your local repository.
It can be done with the following command:

```
mvn install:install-file -DgroupId=org.rosuda.JRI -DartifactId=Rengine -Dversion=$R_VERSION file=$R_LIBRARY/java/jri/REngine.jar
```

The R package, ready for installation in R (with install.packages), 
will be located there. Moreover, each artifact you build will have support for the R 
environment.

If you want to build the RESTful web services for VTL along with the engine, activate the
`with-rest` maven profile. A WAR file ready to be deployed in your application server
will be packaged during the `package` maven lifecycle phase in your build.

If you want to build the command line interface to VTL along with the engine, activate the
`with-cli` maven profile. An executable JAR file ready to be deployed in your platform 
will be packaged during the `package` maven lifecycle phase in your build.

If you want to build the Jupyter notebook kernel along with the engine, also activate the 
`with-jupyter` maven profile. The java-native jupyter kernel will be packaged as an
executable jar that can be installed into your jupyter with the command
`java -jar vtl-jupyter-x.x.x-complete.jar`.

## Project Status

The Project is an ongoing development effort at Bank of Italy, with the 
purpose of demonstrating the feasibility and usefulness of VTL in a real 
scenario, such as translating validation rules exchanged by entities 
operating in the ESCB. Thus,

> some VTL operators and statements offer only limited functionality,
and some have to be implemented yet.

The following table shows the current implementation status:

| operator | scalar | dataset-level | component-level |
| -------- | ------ | ------------- | --------------- |
| define datapoint ruleset    | &#10008;  | &#10008;  | &#10008;  |
| define hierarchical ruleset | &#10008;  | &#10008;  | &#10008;  |
| define operator             | &#10004;  | &#10004;  | &#10004;  |
| call defined operator       | &#10004;  | &#10004;  | &#10004;  |
| persistent assignment       | &#10004;  | &#10004;  | &#10004;  |
| membership (#)              |           | &#10004;  |           |
| eval                        | &#10008;  | &#10008;  | &#10008;  |
| cast                        | &#10004;  |           | &#10004;  |
| inner_join                  |           | &#10004;  |           |
| left_join                   |           | &#10004;  |           |
| full_join                   |           | &#10008;  |           |
| cross_join                  |           | &#10008;  |           |
| concat (&#124;&#124;)       | &#10004;  | &#10004;  | &#10004;  |
| l-r-trim                    | &#10004;  | &#10004;  | &#10004;  |
| upper,lower                 | &#10004;  | &#10004;  | &#10004;  |
| substr                      | &#10004;  | &#10004;  | &#10004;  |
| replace                     | &#10004;  | &#10004;  | &#10004;  |
| instr                       | &#10004;  | &#10004;  | &#10004;  |
| length                      | &#10004;  | &#10004;  | &#10004;  |
| arithmetic                  | &#10004;  | &#10004;  | &#10004;  |
| mod                         | &#10004;  | &#10004;  | &#10004;  |
| round                       | &#10004;  | &#10004;  | &#10004;  |
| trunc                       | &#10004;  | &#10004;  | &#10004;  |
| ceil                        | &#10004;  | &#10004;  | &#10004;  |
| floor                       | &#10004;  | &#10004;  | &#10004;  |
| abs                         | &#10004;  | &#10004;  | &#10004;  |
| exp                         | &#10004;  | &#10004;  | &#10004;  |
| ln                          | &#10004;  | &#10004;  | &#10004;  |
| power                       | &#10004;  | &#10004;  | &#10004;  |
| log                         | &#10004;  | &#10004;  | &#10004;  |
| sqrt                        | &#10004;  | &#10004;  | &#10004;  |
| comparisons (=, >, <, <>)   | &#10004;  | &#10004;  | &#10004;  |
| between                     | &#10004;  | &#10004;  | &#10004;  |
| in/not_in                   | &#10008;  |           | &#10004;  |
| match_characters            | &#10004;  | &#10008;  | &#10004;  |
| isnull                      | &#10004;  | &#10004;  | &#10004;  |
| exists_in                   |           | &#10004;  |           |
| boolean operators           | &#10004;  | &#10004;  | &#10004;  |
| period_indicator            | &#10008;  | &#10008;  | &#10008;  |
| fill_time_series            |           | &#10004;  |           |
| flow_to_stock               |           | &#10004;  |           |
| stock_to_flow               |           | &#10004;  |           |
| timeshift                   |           | &#10004;  |           |
| timeagg                     | &#10008;  | &#10008;  | &#10008;  |
| current_date                | &#10004;  |           | &#10004;  |
| union                       |           | &#10004;  |           |
| intersect                   |           | &#10004;  |           |
| setdiff                     |           | &#10004;  |           |
| symdiff                     |           | &#10004;  |           |
| hierarchy                   |           | &#10004;  |           |
| count                       |           | &#10004;  | &#10004;  |
| min                         |           | &#10004;  | &#10004;  |
| max                         |           | &#10004;  | &#10004;  |
| median                      |           | &#10004;  | &#10004;  |
| sum                         |           | &#10004;  | &#10004;  |
| avg                         |           | &#10004;  | &#10004;  |
| stddev_pop                  |           | &#10004;  | &#10004;  |
| stddev_samp                 |           | &#10004;  | &#10004;  |
| var_pop                     |           | &#10004;  | &#10004;  |
| var_samp                    |           | &#10004;  | &#10004;  |
| first_value                 |           | &#10004;  | &#10004;  |
| last_value                  |           | &#10004;  | &#10004;  |
| lag                         |           | &#10004;  | &#10004;  |
| lead                        |           | &#10004;  | &#10004;  |
| rank                        |           | &#10004;  | &#10004;  |
| ratio_to_report             |           | &#10004;  | &#10004;  |
| check_datapoint             |           | &#10008;  |           |
| check_hierarchy             |           | &#10008;  |           |
| check                       |           | &#10004;  |           |
| if-then-else                | &#10008;  | &#10004;  | &#10004;  |
| nvl                         | &#10008;  |           | &#10004;  |
| filter                      |           |           | &#10004;  |
| calc                        |           |           | &#10004;  |
| aggr                        |           |           | &#10004;  |
| keep/drop                   |           |           | &#10004;  |
| rename                      |           |           | &#10004;  |
| pivot                       |           |           | &#10004;  |
| unpivot                     |           |           | &#10004;  |
| sub                         |           |           | &#10004;  |
## Copyright notice

This software is a copyright of Bank of Italy, inc. 2019.<br/>
The software is distributed under the European Public Licence v1.2.<br/>
This software is optimized using [JProfiler](https://www.ej-technologies.com/products/jprofiler/overview.html).
