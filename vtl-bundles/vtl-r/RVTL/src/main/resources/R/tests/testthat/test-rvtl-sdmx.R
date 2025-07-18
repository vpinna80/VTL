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

testthat::test_that("SDMX env + SDMX repo", {
    vtlLogLevel("off")

    tryCatch(
        {
            session <- "sdmx"
            set_vtl_sdmx_properties(session)
            vtlAddStatements(session, "ds_sdmx:=BIS:WS_EER(1.0):D.N.B.IT;")
            testthat::expect_true(vtlCompile(session), label = "compilation")
            ds_sdmx <- vtlEvalNodes(session, "ds_sdmx")[["ds_sdmx"]]

            !is.null(ds_sdmx) & nrow(ds_sdmx) > 0 & ncol(ds_sdmx) > 0

            testthat::expect_false(is.null(ds_sdmx), label = "first check")
            testthat::expect_gt(nrow(ds_sdmx), 0, label = "second check")
            testthat::expect_gt(ncol(ds_sdmx), 0, label = "third check")
        },
        error = handle_error
    )
})

testthat::test_that("CSV env + json and SDMX CL repo", {
    vtlLogLevel("off")

    tryCatch(
        {
            session <- "csvenvsdmxjson"
            set_vtl_sdmx_csv_properties(session)
            vtlAddStatements(session, "ds_csv_sdmx:=ds_csv;")
            testthat::expect_true(vtlCompile(session), label = "compilation")
            ds_sdmx <- vtlEvalNodes(session, "ds_csv_sdmx")[["ds_csv_sdmx"]]
            ds_csv <- vtlEvalNodes(session, "ds_csv")[["ds_csv"]]

            ds_csv[ds_csv$Id_2 == "A" & ds_csv$Id_1 == 11, "Me_1"] == ds_sdmx[ds_sdmx$Id_2 == "A" & ds_sdmx$Id_1 == 11, "Me_1"]


            testthat::expect_equal(
                ds_csv[ds_csv$Id_2 == "A" & ds_csv$Id_1 == 11, "Me_1"],
                ds_sdmx[ds_sdmx$Id_2 == "A" & ds_sdmx$Id_1 == 11, "Me_1"],
                label = "first check"
            )
        },
        error = handle_error
    )
})

testthat::test_that("R env + json and SDMX DSD repo", {
    vtlLogLevel("off")

    tryCatch(
        {
            session <- "renv+json+sdmx-dsd"
            set_vtl_sdmx_dsd_properties(session)
            # ds_r <<- RJSDMX::getTimeSeriesTable('BIS_PUBLIC', 'WS_EER/D.N.B.IT')
            ds_local <- readRDS(testthat::test_path("data", "ds_r.Rdata"))
            ds_r <<- ds_local # fix R global env
            vtlAddStatements(session, "ds_sdmx:=ds_r;")
            testthat::expect_true(vtlCompile(session), label = "compilation")
            
            ds_sdmx <- vtlEvalNodes(session, "ds_sdmx")[["ds_sdmx"]]
            ds_sdmx[ds_sdmx$TIME_PERIOD == "2005-04-15", "OBS_VALUE"] == ds_r[ds_r$TIME_PERIOD == "2005-04-15", "OBS_VALUE"]

            testthat::expect_equal(
                ds_sdmx[ds_sdmx$TIME_PERIOD == "2005-04-15", "OBS_VALUE"],
                ds_r[ds_r$TIME_PERIOD == "2005-04-15", "OBS_VALUE"],
                label = "first check"
            )
        },
        error = handle_error
    )
})
