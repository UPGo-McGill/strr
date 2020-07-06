#### TESTS FOR strr_raffle #####################################################

### Setup ######################################################################

context("strr_raffle tests")

load("strr_raffle_data.Rdata")


### Tests ######################################################################

result <- strr_raffle(points, polys, GeoUID, dwellings)
result_diagnostic <-
  strr_raffle(points, polys, GeoUID, dwellings, diagnostic = TRUE)

test_that("The function successfully completes", {
  expect_equal(nrow(result), 31)
})

test_that("The geometry column is last", {
  expect_equal(dplyr::last(names(result)), "geometry")
})

test_that("The quiet flag suppresses all messages", {
  expect_message(strr_raffle(points, polys, GeoUID, dwellings, quiet = TRUE),
                 regexp = NA)
})

test_that("The function completes with diagnostic = TRUE", {
  expect_equal(nrow(result_diagnostic), 31)
})

test_that("Points outside the polygons return NA", {
  expect_equal(result[31,]$GeoUID, NA_character_)
})

test_that("Raffle candidates are correct", {
  expect_equal(nrow(result_diagnostic[9,]$candidates[[1]]), 4)
  expect_equal(nrow(result_diagnostic[1,]$candidates[[1]]), 1)
})

test_that("Odd field names are handled properly", {
  # points with field named {{ poly_ID }}
  expect_equal(nrow(strr_raffle(
    dplyr::rename(points, GeoUID = property_ID), polys, GeoUID, dwellings)), 31)
  # points with field name "poly_ID"
  expect_equal(nrow(strr_raffle(
    dplyr::rename(points, poly_ID = property_ID), polys, GeoUID, dwellings)),
    31)
  # points and polys with field name "poly_ID"
  expect_equal(nrow(strr_raffle(
    dplyr::rename(points, poly_ID = property_ID),
    dplyr::rename(polys, poly_ID = GeoUID), poly_ID, dwellings)), 31)
})
