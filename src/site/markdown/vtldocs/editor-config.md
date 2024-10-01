# Configuring VTL engine settings in VTL Editor

This section explains how to configure VTL engine settings in the editor.

The settings tab is divided into four panels.

## VTL Engine

![vtl-settings-engine](images/settings-engine.png)

The panel on the top left allows to select the VTL engine.

At this moment there is only one available, so there's nothing to do here.

## Metadata Repository

![vtl-settings-repo](images/settings-repo.png)

The panel on the top right allows to select how to initialize the metadata repository.

The repository contains the domain definitions available to all sessions. 
There are three initializations available:

*  **In-Memory**: Basic environment, with only the standard VTL domains and no initialization;
*  **CSV File**: Initialization with codelists from a specified CSV file;
*  **SDMX Registry**: Initialization with codelists from a specified SDMX 2.1 compliant web service.

The initialization method must be chosen before any VTL code is compiled.
Changing the repository later would have no effect.

## Environments

![vtl-settings-envs](images/settings-envs.png)

The panel on the bottom left allows to choose where the VTL session will look up
for values. In the current version all the environments are already enabled.

To disable an environment, drag it to the "Available" box, and to enable it drag it
again to the "Active" box. You may also sort the environments based on your preference.

However, in any circumstance, the In-Memory environment should never be disabled.

## Network Proxy

![vtl-settings-proxy](images/settings-proxy.png)

The panel on bottom right allows to choose a proxy to use for any outbound internet connection.

Enter the details of the proxy connection and press the Save button to set the proxy.