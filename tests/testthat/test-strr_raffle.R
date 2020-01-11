#### TESTS FOR strr_raffle #####################################################

### Setup ######################################################################

context("strr_raffle tests")

#' @importFrom dplyr %>% arrange
#' @importFrom tibble tibble
#' @importFrom sf st_as_sf st_crs st_drop_geometry st_point st_sfc

load("strr_raffle_data.Rdata")


### Tests ######################################################################

test_that("The function successfully completes", {
  expect_equal(nrow(strr_raffle(points, polys, GeoUID, dwellings)), 31)
})

test_that("The geometry column is last", {
  expect_equal(
    dplyr::last(names(strr_raffle(points, polys, GeoUID, dwellings))),
    "geometry")
})

test_that("The quiet flag suppresses all messages", {
  expect_message(strr_raffle(points, polys, GeoUID, dwellings, quiet = TRUE),
                 regexp = NA)
})

test_that("The function completes with diagnostic = TRUE", {
  expect_equal(nrow(
    strr_raffle(points, polys, GeoUID, dwellings, diagnostic = TRUE)
    ), 31)
})

# test_that("Raffle candidates are correct", {
#   expect_equal(
#     strr_raffle(points, polys, GeoUID, dwellings, diagnostic = TRUE)
#     )
# })
