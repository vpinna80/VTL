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
.onAttach <- function(libname, pkgname) {

  spark <- Sys.getenv('SPARK_HOME')
  jars = c("log4j2.xml", system.file("java/log4j2.xml", package = pkgname), list.files(system.file("java", package = pkgname), ".*\\.jar"))
  files <- if (spark != '') list.files(paste0(spark, '/jars'), full.names = T) else ''
  files <- c(files, list.files(paste0(find.package("rJava"), "/jri/"), pattern = '.*\\.jar', full.names = T))
  files <- files[!grepl("log4j", files)]
  files <- files[!grepl("slf4j", files)]
  
  .jpackage(pkgname, jars, files, lib.loc = libname)

  version.string <- J("java.lang.System")$getProperty("java.version")
  java.version <- as.integer(gsub("^([0-9]*)\\..*$", "\\1", version.string))
  if (java.version != 11)
    stop("RVTL requires a Java 11 virtual machine, but version '", version.string , "' was found.")
  
  J("java.lang.System")$setProperty("vtl.environment.implementation.classes", paste(sep = ",",
      "it.bancaditalia.oss.vtl.impl.environment.REnvironment")
  )

  if (!any(grepl("roxygenize", sys.calls())) && !any(grepl("test_load_package", sys.calls())))
  {
    packageStartupMessage(paste0("JRI Rengine initialized: ", J("it.bancaditalia.oss.vtl.impl.environment.RUtils")$RENGINE$hashCode()))
    propfile = paste0(J("java.lang.System")$getProperty("user.home"), '/.vtlStudio.properties')
    if (file.exists(propfile)) {
      reader = .jnew("java.io.FileReader", propfile)
      tryCatch({
        J("it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory")$loadConfiguration(reader)
        packageStartupMessage("VTL settings loaded from ", propfile, ".")
      }, finally = {
        reader$close()
      })
    }
  }
}
