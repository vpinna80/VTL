#
# Copyright © 2020 Banca D'Italia
#
# Licensed under the EUPL, Version 1.2 (the "License");
# You may not use this work except in compliance with the
# License.
# You may obtain a copy of the License at:
#
# https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
#
# Unless required by applicable law or agreed to in
# writing, software distributed under the License is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
#
# See the License for the specific language governing
# permissions and limitations under the License.
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
