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

list_to_dot_vtl_properties_file <- function(filepath, vtl_list) {
  if (is.null(filepath)) {
    filepath <- ".vtlProperties.config"
  }
  if (is.null(vtl_list)) {
    vtl_list <- list()
  }
  cat("", file = filepath) # overwrite file if exists
  for (prop in names(vtl_list)) {
    cat(sprintf("%s=%s\n", prop, vtl_list[[prop]]), file = filepath, append = TRUE)
  }
  return(filepath)
}

path_to_file_url <- function(path) {
  sprintf("file\\://%s", file.path(path))
}

set_vtl_properties <- function() {
  vtlprop_conf <- list(
    "vtl.config.use.bigdecimal" = "false",
    "vtl.engine.implementation.class" = "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine",
    "vtl.config.impl.class" = "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl",
    "vtl.session.implementation.class" = "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl"
  )
  propfile <- list_to_dot_vtl_properties_file(filepath = tempfile(), vtlprop_conf)
  RVTL::VTLSessionManager$load_config(propfile = propfile)
}

set_vtl_r_json_properties <- function() {
  vtl_prop <- list(
    "vtl.metadatarepository.class" = "it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository",
    "vtl.environment.implementation.classes" = "it.bancaditalia.oss.vtl.impl.environment.REnvironment",
    "vtl.engine.implementation.class" = "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine",
    "vtl.config.impl.class" = "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl",
    "vtl.json.metadata.url" = path_to_file_url(system.file("tests", "testthat", "data", "ex_r_json.json", package = "RVTL")),
    "vtl.session.implementation.class" = "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl",
    "vtl.config.use.bigdecimal" = "false"
  )
  propfile <- list_to_dot_vtl_properties_file(filepath = tempfile(), vtl_list = vtl_prop)
  RVTL::VTLSessionManager$load_config(propfile = propfile)
}

set_vtl_csv_json_properties <- function() {
  vtl_prop <- list(
    "vtl.metadatarepository.class" = "it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository",
    "vtl.environment.implementation.classes" = "it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment",
    "vtl.engine.implementation.class" = "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine",
    "vtl.config.impl.class" = "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl",
    "vtl.json.metadata.url" = path_to_file_url(system.file("tests", "testthat", "data", "ex_csv.json", package = "RVTL")),
    "vtl.csv.search.path" = system.file("tests", "testthat", "data", package = "RVTL"),
    "vtl.session.implementation.class" = "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl",
    "vtl.config.use.bigdecimal" = "false"
  )
  propfile <- list_to_dot_vtl_properties_file(filepath = tempfile(), vtl_list = vtl_prop)
  RVTL::VTLSessionManager$load_config(propfile = propfile)
}

proxy_to_vtl_properties <- function() {
  proxy_host <- Sys.getenv("RVTL_TEST_PROXY_HOST")
  proxy_port <- Sys.getenv("RVTL_TEST_PROXY_PORT")
  proxy_prop <- list()
  if (proxy_host != "") {
    proxy_prop[["http.proxyHost"]] <- proxy_host
    proxy_prop[["https.proxyHost"]] <- proxy_host
  }

  if (proxy_port != "") {
    proxy_prop[["http.proxyPort"]] <- proxy_port
    proxy_prop[["https.proxyPort"]] <- proxy_port
  }

  return(proxy_prop)
}

set_vtl_sdmx_properties <- function() {
  vtl_prop <- list(
    "vtl.metadatarepository.class" = "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository",
    "vtl.sdmx.meta.version" = "1.5.0",
    "vtl.sdmx.meta.password" = "",
    "vtl.engine.implementation.class" = "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine",
    "vtl.config.impl.class" = "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl",
    "vtl.session.implementation.class" = "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl",
    "vtl.sdmx.data.user" = "",
    "vtl.environment.implementation.classes" = "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment",
    "vtl.sdmx.data.endpoint" = "https\\://stats.bis.org/api/v1",
    "vtl.sdmx.meta.user" = "",
    "vtl.sdmx.meta.endpoint" = "https\\://stats.bis.org/api/v1",
    "vtl.config.use.bigdecimal" = "false",
    "vtl.sdmx.data.password" = ""
  )
  proxy_prop <- proxy_to_vtl_properties()
  vtl_prop <- c(vtl_prop, proxy_prop)
  propfile <- list_to_dot_vtl_properties_file(filepath = tempfile(), vtl_list = vtl_prop)
  RVTL::VTLSessionManager$load_config(propfile = propfile)
}

set_vtl_sdmx_csv_properties <- function() {
  vtl_prop <- list(
    "vtl.metadatarepository.class" = "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository",
    "vtl.sdmx.meta.version" = "1.5.0",
    "vtl.sdmx.meta.password" = "",
    "vtl.engine.implementation.class" = "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine",
    "vtl.config.impl.class" = "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl",
    "vtl.session.implementation.class" = "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl",
    "vtl.environment.implementation.classes" = "it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment",
    "vtl.sdmx.meta.user" = "",
    "vtl.sdmx.meta.endpoint" = "https\\://stats.bis.org/api/v1",
    "vtl.config.use.bigdecimal" = "false",
    "vtl.sdmx.data.password" = "",
    "vtl.json.metadata.url" = path_to_file_url(system.file("tests", "testthat", "data", "ex_csv.json", package = "RVTL")),
    "vtl.csv.search.path" = system.file("tests", "testthat", "data", package = "RVTL")
  )
  proxy_prop <- proxy_to_vtl_properties()
  vtl_prop <- c(vtl_prop, proxy_prop)
  propfile <- list_to_dot_vtl_properties_file(filepath = tempfile(), vtl_list = vtl_prop)
  RVTL::VTLSessionManager$load_config(propfile = propfile)
}

set_vtl_sdmx_dsd_properties <- function() {
  vtl_prop <- list(
    "vtl.metadatarepository.class" = "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository",
    "vtl.sdmx.meta.version" = "1.5.0",
    "vtl.sdmx.meta.password" = "",
    "vtl.engine.implementation.class" = "it.bancaditalia.oss.vtl.impl.engine.JavaVTLEngine",
    "vtl.config.impl.class" = "it.bancaditalia.oss.vtl.impl.config.ConfigurationManagerImpl",
    "vtl.session.implementation.class" = "it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl",
    "vtl.environment.implementation.classes" = "it.bancaditalia.oss.vtl.impl.environment.REnvironment",
    "vtl.sdmx.meta.user" = "",
    "vtl.json.metadata.url" = path_to_file_url(system.file("tests", "testthat", "data", "ex_sdmx_dsd.json", package = "RVTL")),
    "vtl.sdmx.meta.endpoint" = "https\\://stats.bis.org/api/v1",
    "vtl.config.use.bigdecimal" = "false"
  )
  proxy_prop <- proxy_to_vtl_properties()
  vtl_prop <- c(vtl_prop, proxy_prop)
  propfile <- list_to_dot_vtl_properties_file(filepath = tempfile(), vtl_list = vtl_prop)
  RVTL::VTLSessionManager$load_config(propfile = propfile)
}
