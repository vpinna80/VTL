\name{vtlGetCode}
\alias{vtlGetCode}
\title{prints the VTL statements of the specified VTL session}
\usage{
vtlGetCode(sessionID)
}
\arguments{
  \item{sessionID}{the session identifier}
}
\description{
This function is used to get the VTL statements in the specified VTL session.
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
vtlGetCode('test')

}
}

