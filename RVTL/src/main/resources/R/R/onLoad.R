# Copyright 2019,2019 Bank Of Italy
#
# Licensed under the EUPL, Version 1.1 or - as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
#
#
# http://ec.europa.eu/idabc/eupl
#
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
#

#' @import rJava
#' @import R6
.onLoad <- function(libname, pkgname) {
  .jpackage(pkgname, lib.loc = libname)
  J("java.lang.System")$setProperty("vtl.environment.implementation.classes", paste(sep = ",",
      "it.bancaditalia.oss.vtl.impl.environment.CSVFileEnvironment",
      "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment",
      "it.bancaditalia.oss.vtl.impl.environment.REnvironment",
      "it.bancaditalia.oss.vtl.impl.environment.WorkspaceImpl")
  )
}
