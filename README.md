[![License](https://img.shields.io/badge/license-EUPL-green)](https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12)
![VTL E&E builds](https://github.com/vpinna80/VTL/actions/workflows/maven.yml/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/c20a3a19b6744db191d9dd1b1b3a8cbf)](https://www.codacy.com/manual/valentino.pinna/VTL?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=vpinna80/VTL&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/c20a3a19b6744db191d9dd1b1b3a8cbf)](https://app.codacy.com/gh/vpinna80/VTL/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_coverage)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/vpinna80/VTL?label=github-release)
![Maven metadata URL](https://img.shields.io/maven-metadata/v?label=maven-release&metadataUrl=https%3A%2F%2Frepo1.maven.org%2Fmaven2%2Fit%2Fbancaditalia%2Foss%2Fvtl%2Fvtl%2Fmaven-metadata.xml)

# VTL E&E

An engine and editor for the 
[Validation and Transformation Language](https://sdmx.org/?page_id=5096), 
written in Java, Javascript and R.

## Usage and documentation

Usage info, documentation and examples are available at the
[project wiki](https://github.com/vpinna80/VTL/wiki).

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

    mvn [-P with-r,with-sdmx,with-spark,with-jupyter,with-cli] clean package

Each artifact will be generated inside the `target` folder of each module.
The optional maven profiles allow you to build any of the provided VTL bundles for
the different front-ends capable of communicating with the VTL engine.

### RVTL and VTL Studio

If you want to build the editor and the R package along with the engine, activate the 
`with-r` maven profile. Before you do that, make sure to install `rJava` package in your 
R installation. Then you may need to configure your internet connection in Maven settings
in order to download Node.js and npm packages. You will also need to install `REngine.jar`
into your local maven repository. It can be done with the following command:

```
Un*x:
mvn install:install-file -DgroupId=org.rosuda.JRI -DartifactId=Rengine -Dversion=$R_VERSION -Dfile=$R_LIBRARY/java/jri/REngine.jar -Dpackaging=jar

Windows
mvn install:install-file -DgroupId=org.rosuda.JRI -DartifactId=Rengine -Dversion=%R_VERSION% -Dfile=%R_LIBRARY%/java/jri/REngine.jar -Dpackaging=jar
```

Please set the environment variables `R_VERSION` and `R_LIBRARY` accordingly
to your environment settings, or directly replace them in the command above.
The R package, ready for installation in R (with install.packages), 
will be located there. Moreover, each bundle created while this profile is
active will contain support for the R environment.

### SDMX Support

VTL E&E is fully integrated with sdmx.io Metadata Registry. In order to enable support in 
built bundles for SDMX interactions, such as using DSD and datasets, activate the `with-sdmx`
maven profile. To build you also need to have previously built and installed the metadata 
registry maven artifacts, available from sdmx.io web site. 

### Spark support

If you want to build with support for the Spark evironment, activate the 
`with-spark` maven profile. Each bundle listed below created while this profile is
active will contain the Spark environment (Note that the Spark libraries are not
included in the bundle and must be made available at runtime in your CLASSPATH).

### Jupyter notebook kernel

If you want to build the Jupyter notebook kernel along with the engine, also activate the 
`with-jupyter` maven profile. The java-native jupyter kernel will be packaged as an
executable jar that can be installed into your jupyter with the command
`java -jar vtl-jupyter-x.x.x-complete.jar`. Please make sure that your Python environment
is able to launch java, and that java version requirement is satisfied.

### Command-line interface for VTL Engine

If you want to build the command line interface to VTL along with the engine, activate the
`with-cli` maven profile. An executable JAR file ready to be deployed in your platform 
will be packaged during the `package` maven lifecycle phase in your build.

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
| define datapoint ruleset    |     |     | ❌  |
| define hierarchical ruleset | ✅  |     |     |
| define operator             | ✅  | ✅  | ✅  |
| call defined operator       | ✅  | ✅  | ✅  |
| persistent assignment       | ✅  | ✅  | ✅  |
| membership (#)              |     | ✅  |     |
| eval                        | ❌  | ❌  | ❌  |
| cast                        | ✅  |     | ✅  |
| pivot/unpivot               |     | ✅  |     |
| filter, sub                 |     | ✅  |     |
| keep, drop, rename          |     | ✅  |     |
| calc, aggr                  |     | ✅  |     |
| inner_join                  |     | ✅  |     |
| left_join                   |     | ✅  |     |
| full_join                   |     | ✅  |     |
| cross_join                  |     | ✅  |     |
| check                       | ✅  |     |     |
| check_datapoint             | ✅  |     |     |
| check_hierarchy             | ✅  |     |     |
| hierarchy                   | ✅  |     |     |
| set operations              | ✅  |     |     |
| if-then-else                | ✅  | ✅  | ✅  |
| case when                   | ✅  | ✅  | ✅  |
| l-r-trim                    | ✅  | ✅  | ✅  |
| upper,lower                 | ✅  | ✅  | ✅  |
| substr                      | ✅  | ✅  | ✅  |
| replace                     | ✅  | ✅  | ✅  |
| instr                       | ✅  | ✅  | ✅  |
| match_characters            | ✅  | ❌  | ✅  |
| length                      | ✅  | ✅  | ✅  |
| arithmetic (+, -, *, /)     | ✅  | ✅  | ✅  |
| mod                         | ✅  | ✅  | ✅  |
| boolean operators           | ✅  | ✅  | ✅  |
| rounding functions          | ✅  | ✅  | ✅  |
| math functions              | ✅  | ✅  | ✅  |
| comparisons (=, >, <, <>)   | ✅  | ✅  | ✅  |
| between                     | ✅  | ✅  | ✅  |
| in/not_in                   | ❌  |     | ✅  |
| isnull                      | ✅  | ✅  | ✅  |
| nvl                         | ✅  | ✅  | ✅  |
| exists_in                   |     | ✅  |     |
| period_indicator            | ✅  | ✅  | ✅  |
| fill_time_series            |     | ✅  |     |
| flow_to_stock               |     | ✅  |     |
| stock_to_flow               |     | ✅  |     |
| timeshift                   |     | ✅  |     |
| timeagg                     | ✅  | ✅  | ✅  |
| current_date                | ✅  |     | ✅  |
| dateadd, datediff           |     | ✅  |     |
| yeartoday, daytoyear        |     | ❌  |     |
| monthtoday, daytomonth      |     | ❌  |     |
| getyear, getmonth, getday   |     | ✅  |     |
