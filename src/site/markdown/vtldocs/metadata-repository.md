# TODO
# What's the Metadata Repository
In a VTL program, data and metadata lives in different places, the metadata repository is an external service that the application can query to request the metadata. (TODO)

# Which Metadata Repository are supported
VTL Studio support the following metadata repository:
*  **In-Memory**: Basic environment, with only the standard VTL domains and no initialization;
*  **CSV File**: Initialization with codelists from a specified CSV file;
*  **SDMX Registry**: Initialization with codelists from a specified SDMX 2.1 compliant web service.

# How to select the metadata repository

![vtl-settings-repo](images/settings-repo.png)

The panel on the top right allows to select how to initialize the metadata repository.
The repository contains the domain definitions available to all sessions. 
The initialization method must be chosen before any VTL code is compiled.
Changing the repository later would have no effect.
