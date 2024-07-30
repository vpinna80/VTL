# Get Started

This section explains how to fast forward into productivity with vtlStudio, the embedded VTL editor for R.

## Installation

A release to CRAN is planned. In the meantime, just open an R session and submit the following command:

```R
install.packages(c('rJava', 'R6', 'RJSDMX', 'igraph', 'networkD3', 'DT',  
        'shiny', 'shinydashboard', 'shinyjs', 'sortable', 'jdx', 'xtable', 'dplyr'))
install.packages('${scmUrl}/releases/download/v${project.version}/RVTL_${project.version}.tar.gz', repos = NULL)
```

R will download the package from GitHub and install it on your machine with all required dependencies.

## Startup

To start the editor, from the R console, launch the commands:

```R
> library(RVTL)
> vtlStudio()

Listening on http://127.0.0.1:4384
```

Then, your default browser will open a page to the vtlStudio web application.
If vtlStudio does not automatically open, just paste the output link in the R console to start it up.

## Creating a session

The first thing to do when opening vtlStudio is to create a session.
On the sidebar, type a name of your choice into the New Session box, and click the Create button.

![create-session](images/create-session.png)

You will see the newly created session on the Session ID dropdown menu.

## Submitting code

For a first run, enter the following VTL code in the editor:

```
a := "Hello World!";
```

then submit the code by clicking the Compile and Submit button.

After a moment, you will see the following message in the gray output box:

![hello-world-compile](images/hello-world-compile.png)

## Browse results

The Dataset Explorer tab allows you to browse the computed values of any submitted rule.

You can choose the rule to compute and show with the Select Node dropdown menu.

In this case, select the **a** node. You will see the following:

![hello-world-result](images/hello-world-result.png)

