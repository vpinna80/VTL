# TODO
# What's an Environment?
In VTL an Environment is a supported data source, where a VTL application can read the data that it's needed for its correct execution.
VTL Studio support numerous Environments like:
- **In-Memory Environment**
- **CSV Environment**
- **SDMX Environment**
- **R Environment**
- **Apache Spark Environment**

# How to select an Environment inside VTL Studio

![vtl-settings-envs](images/settings-envs.png)

The panel on the bottom left allows to choose where the VTL session will look up
for values. In the current version all the environments are already enabled.

To disable an environment, drag it to the "Available" box, and to enable it drag it
again to the "Active" box. You may also sort the environments based on your preference.

However, in any circumstance, the In-Memory environment should never be disabled.
