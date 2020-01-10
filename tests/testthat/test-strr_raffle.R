#### TESTS FOR strr_raffle #####################################################

### Setup ######################################################################

context("strr_raffle tests")

#' @importFrom dplyr %>% arrange
#' @importFrom tibble tibble
#' @importFrom sf st_as_sf st_crs st_drop_geometry st_point st_sfc

load("strr_raffle_data.Rdata")


### Tests ######################################################################

test_that("function successfully completes", {
  # cores
  expect_equal(nrow(strr_raffle(points, polys, GeoUID, dwellings)), 31)
})

test_that("The quiet flag suppresses all messages", {
  expect_message(strr_raffle(points, polys, GeoUID, dwellings, quiet = TRUE),
                 regexp = NA)
})
