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

    mvn [-P with-r,with-fmr,with-spark,with-jupyter,with-cli,with-rest] clean package

Each artifact will be generated inside the `target` folder of each module.
The optional maven profiles allow you to build any of the provided VTL bundles for
the different front-ends capable of communicating with the VTL engine.

If you want to build with support for the Spark evironment, activate the 
`with-spark` maven profile. Each bundle created with this build will contain the 
Spark environment.

If you want to build with support for SDMX web services, activate the
`with-fmr` maven profile. Each bundle created with this build will contain the support
for interacting with a SDMX registry. Note that this requires that the 
[sdmx-core](https://github.com/bis-med-it/sdmx-core) maven artifacts are already installed 
in the local repository, because the artifacts are distributed only in source form 
by the Bank of International Settlements.

If you want to build the editor and the R package along with the engine, activate the 
`with-r` maven profile; you may need to configure your internet connection in 
Maven settings. The R package, ready for installation in R (with install.packages), 
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

The following map shows the current implementation status:
![statusmap](https://vpinna80.github.io/VTL/images/VTL.png)

## Copyright notice

This software is a copyright of Bank of Italy, inc. 2019.<br/>
The software is distributed under the European Public Licence v1.2.<br/>
This software is optimized using [JProfiler](https://www.ej-technologies.com/products/jprofiler/overview.html).
