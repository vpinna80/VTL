\name{vtlEvalNodes}
\alias{vtlEvalNodes}
\title{calculate and return the values of the nodes of a VTL session}
\usage{
vtlEvalNodes(sessionID, nodes)
}
\arguments{
  \item{sessionID}{identifier of the session}

  \item{nodes}{the list of nodes to be evaluated}

}
\description{
This function is used to evaluate specific nodes of a vtl session. The evaluated nodes 
will be returned in a list.
}
\examples{
\dontrun{
#prepare a VTL compliant dataset in R
r_input <- data.frame(id1 = c('a', 'b', 'c'), m1 = c(1.23, -2.34, 3.45), m2 = c(1.23, -2.34, 3.45), stringsAsFactors = F)
attr(r_input, 'identifiers') <- c('id1')
attr(r_input, 'measures') <- c('m1', 'm2')

vtlAddStatements(session = 'test', restartSession = T,
                  statements = 'a := r_input;
                                b := 3;
                                c := abs(sqrt(14 + a));
                                d := a + b + c;
                                e := c + d / a;
                                f := e - d;
                                g := -f;
                                test := a * b + c / a;')

vtlCompile('test')
vtlEvalNodes('test', vtlListNodes('test'))                    
}
}

