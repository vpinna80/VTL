\name{vtlCompile}
\alias{vtlCompile}
\title{compiles the VTL statements of the specified VTL session}
\usage{
vtlCompile(sessionID)
}
\arguments{
  \item{sessionID}{the session identifier}
}
\description{
This function is used to compile the VTL statements in the specified VTL session. Compilation does not only check the syntax: it also ensures structural consistency.
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

}
}

