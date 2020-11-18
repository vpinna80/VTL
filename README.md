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

A complete description of the project modules and the build process will be aailable soon.

To build this project, you will need:

* A Windows or Linux machine;

> Building on a MacOS machine should be possible by now but **it is untested**.

* JDK >= 8;
* [Apache Maven](https://maven.apache.org/) 3.6.3;
* [GNU R](https://www.r-project.org/) >= 3.5.3 for building RVTL package;
* [Python 3.7](https://www.python.org/) >= 3.7 for building the Jupyter notebook kernel;
* Configured internet connection (to download [node.js](https://nodejs.org/)).

To build the project, launch the command:

    mvn [-P with-r,with-python] [-Dsdmx.version=x.x.x] clean package


Each artifact will be generated inside the `target` folder of each module.

If you want to build the editor and the R package along with the engine, activate the 
`with-r` maven profile; you may need to configure your internet connection in 
Maven. The R package, ready for installation in R (with install.packages), will 
be located there.

If you want to build the Jupyter notebook kernel along with the engine, also activate the 
`with-python` maven profile; you may need to install the prerequisite packages for the
kernel: [`Jpype1`](https://github.com/jpype-project/jpype), [`ipykernel`](https://ipython.org/), 
[`numpy`](https://numpy.org/) and [`pandas`](https://pandas.pydata.org/). Please make
sure that Jpype points to a JDK >= 8.

If you want to use a different version of the 
[SDMX connectors](https://github.com/amattioc/SDMX.git) dependency, change 
the relative property value. The current default version is 2.3.3. [Check 
here](https://search.maven.org/artifact/it.bancaditalia.oss/sdmx) the available maven releases.

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

This software is a copyright of Bank of Italy, 2019-2020.
The software is distributed under the European Public Licence v1.2.
