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


# vtl classes for setting global properties
CONFIGURATION_MANAGER <- J("it.bancaditalia.oss.vtl.config.ConfigurationManager")
VTL_GENERAL_PROPERTIES <- J("it.bancaditalia.oss.vtl.config.VTLGeneralProperties")

# set global properties for default metadata and default environment
set_default_vtl_properties <- function() {
  CONFIGURATION_MANAGER$setGlobalPropertyValue(
    VTL_GENERAL_PROPERTIES$METADATA_REPOSITORY,
    "it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository"
  )
  CONFIGURATION_MANAGER$setGlobalPropertyValue(
    VTL_GENERAL_PROPERTIES$ENVIRONMENT_IMPLEMENTATION,
    ""
  )
}

# in input session set property inside class
set_vtl_property_for_session <- function(session, class, property) {
  VTLSessionManager$getOrCreate(session)$setProperty(class, property)
  session
}


# if error is from java exception print it, otherwise print R error message
handle_error <- function(e) {
  if (!is.null(e$jobj)) {
    e$jobj$printStackTrace()
    testthat::fail(message = "failure caused by java exception.")
  } else {
    testthat::fail(message = e$message)
  }
}

# convert path to file://path
path_to_file_url <- function(path) {
  sprintf("file:///%s", file.path(path))
}


set_vtl_r_properties <- function(session) {
  inMemoryMetadataRepository <- "it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository"
  rEnvironment <- "it.bancaditalia.oss.vtl.impl.environment.REnvironment"
  set_vtl_property_for_session(
    session,
    VTL_GENERAL_PROPERTIES$METADATA_REPOSITORY,
    inMemoryMetadataRepository
  )
  set_vtl_property_for_session(
    session,
    VTL_GENERAL_PROPERTIES$ENVIRONMENT_IMPLEMENTATION,
    rEnvironment
  )
  session
}

set_vtl_r_json_properties <- function(session) {
  jsonMetadataRepository <- "it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository"
  rEnvironment <- "it.bancaditalia.oss.vtl.impl.environment.REnvironment"
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$METADATA_REPOSITORY, jsonMetadataRepository)
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$ENVIRONMENT_IMPLEMENTATION, rEnvironment)
  set_vtl_property_for_session(
    session,
    J(jsonMetadataRepository)$JSON_METADATA_URL,
    path_to_file_url(system.file("tests", "testthat", "data", "ex_r_json.json", package = "RVTL"))
  )
  session
}

set_vtl_csv_json_properties <- function(session) {
  jsonMetadataRepository <- "it.bancaditalia.oss.vtl.impl.meta.json.JsonMetadataRepository"
  csvPathEnvironment <- "it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment"
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$METADATA_REPOSITORY, jsonMetadataRepository)
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$ENVIRONMENT_IMPLEMENTATION, csvPathEnvironment)
  set_vtl_property_for_session(
    session,
    J(jsonMetadataRepository)$JSON_METADATA_URL,
    path_to_file_url(system.file("tests", "testthat", "data", "ex_csv.json", package = "RVTL"))
  )
  set_vtl_property_for_session(
    session,
    J(csvPathEnvironment)$CSV_ENV_SEARCH_PATH,
    system.file("tests", "testthat", "data", package = "RVTL")
  )
  session
}

set_proxy <- function() {
  proxy_host <- Sys.getenv("RVTL_TEST_PROXY_HOST")
  proxy_port <- Sys.getenv("RVTL_TEST_PROXY_PORT")
  jsystem <- J("java.lang.System")
  if (proxy_host != "") {
    jsystem$setProperty("http.proxyHost", proxy_host)
    jsystem$setProperty("https.proxyHost", proxy_host)
  }
  if (proxy_port != "") {
    jsystem$setProperty("http.proxyPort", proxy_port)
    jsystem$setProperty("https.proxyPort", proxy_port)
  }
}

set_vtl_sdmx_properties <- function(session) {
  sdmx_metadata_repository <- "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository"
  sdmx_environment <- "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment"
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$METADATA_REPOSITORY, sdmx_metadata_repository)
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$ENVIRONMENT_IMPLEMENTATION, sdmx_environment)
  set_vtl_property_for_session(
    session,
    J(sdmx_metadata_repository)$SDMX_REGISTRY_ENDPOINT,
    "https://stats.bis.org/api/v1"
  )
  set_vtl_property_for_session(
    session,
    J(sdmx_environment)$SDMX_DATA_ENDPOINT,
    "https://stats.bis.org/api/v1"
  )
  set_proxy()
  session
}

set_vtl_sdmx_csv_properties <- function(session) {
  csvPathEnvironment <- "it.bancaditalia.oss.vtl.impl.environment.CSVPathEnvironment"
  sdmxJsonRepository <- "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository"
  sdmxRepository <- "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository"

  sdmx_metadata_repository <- paste(sdmxRepository, sdmxJsonRepository, sep = ",")

  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$METADATA_REPOSITORY, sdmxJsonRepository)
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$ENVIRONMENT_IMPLEMENTATION, csvPathEnvironment)

  set_vtl_property_for_session(
    session,
    J(sdmxRepository)$SDMX_REGISTRY_ENDPOINT,
    "https://stats.bis.org/api/v1"
  )

  set_vtl_property_for_session(
    session,
    J(sdmxJsonRepository)$JSON_METADATA_URL,
    path_to_file_url(system.file("tests", "testthat", "data", "ex_csv.json", package = "RVTL"))
  )

  set_vtl_property_for_session(
    session,
    J(csvPathEnvironment)$CSV_ENV_SEARCH_PATH,
    system.file("tests", "testthat", "data", package = "RVTL")
  )
  set_proxy()
  session
}

set_vtl_sdmx_dsd_properties <- function(session) {
  sdmxJsonRepository <- "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXJsonRepository"
  sdmxRepository <- "it.bancaditalia.oss.vtl.impl.meta.sdmx.SDMXRepository"
  sdmxEnvironment <- "it.bancaditalia.oss.vtl.impl.environment.SDMXEnvironment"
  rEnvironment <- "it.bancaditalia.oss.vtl.impl.environment.REnvironment"

  sdmx_metadata_repository <- paste(sdmxRepository, sdmxJsonRepository, sep = ",")
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$METADATA_REPOSITORY, sdmxJsonRepository)
  set_vtl_property_for_session(session, VTL_GENERAL_PROPERTIES$ENVIRONMENT_IMPLEMENTATION, rEnvironment)


  set_vtl_property_for_session(
    session,
    J(sdmxJsonRepository)$JSON_METADATA_URL,
    path_to_file_url(system.file("tests", "testthat", "data", "ex_sdmx_dsd.json", package = "RVTL"))
  )
  set_vtl_property_for_session(
    session,
    J(sdmxRepository)$SDMX_REGISTRY_ENDPOINT,
    "https://stats.bis.org/api/v1"
  )
  set_proxy()
  session
}
