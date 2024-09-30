# Using the editor
The VTLStudio application is categorized in four main tabs:
1. **The Editor**
2. **The Structure Explorer**
3. **The Dataset Explorer**
4. **The Graph Explorer**
   
And two more tabs to set up the environemnts and run time configuration of the VTL application:

1. **Engine Settings**
2. **Network Settings**
   
# VTL Editor
The Vtl Editor support a rich set of features aimed at writing VTL applications like:
- **Text Editing features**
- **Inteligent Code Completion**
- **Compiling VTL Application**
- **Visualizing the data and the metadata**
- **Lazy run**
- **Loading Local Data**
- **Theming**
  
## Session
For a VTL application, a session is the union of the VTL application code and the list of Environments needed to run it correctly, we can create, save, and load session from the editor tab with ease.

## Theming
VTLStudio support numerous themes built-in for the editor.

# Structure Explorer
The Structure Explorer allow to query the metadata of the data used by the vtl application.

# Dataset Explorer
The Dataset Explorer allow to examinate the data used by a VTL application directly, we can visualize the values of the data points and, if the data is computed by other data, query for the lineage of the data, to understand from which other data is derived.

# Graph Explorer
The Graph explorer is an advanced graphical tool that visualize the lineages of the data used by a VTL program in a graph format.

# Engine Settings
The engine settings tab allow to set the VTL environments, the metadata repository and the environments properties.
An environements is a data source for the applications, see at [Environments](environments.md)  

# Network Settings
The network settings tab allow to set a network proxy with basic authentication.