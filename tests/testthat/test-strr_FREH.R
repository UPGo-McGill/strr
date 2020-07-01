#### TESTS FOR strr_FREH #######################################################

### Setup ######################################################################

context("strr_FREH tests")

daily <-
  dplyr::tibble(property_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
         date = as.Date(c(
           "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
           "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
           "2018-04-09", "2018-04-10", "2018-04-11", "2018-04-12",
           "2018-04-13", "2018-04-14", "2018-04-15", "2018-04-16",
           "2018-04-17", "2018-04-18", "2018-04-19", "2018-04-20",
           "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
           "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
           "2018-04-09", "2018-04-10")),
         status = c(
           rep("A", 5), rep("R", 2), rep("A", 13),
           rep("A", 4), rep("B", 3), rep("R", 3)),
         booked_date = NA,
         price = rep(100, 30),
         res_ID = NA,
         host_ID = NA,
         listing_type = "Entire home/apt",
         housing = NA,
         country = NA,
         region = NA,
         city = NA,
         longitude = rep(50, 30),
         latitude = rep(50, 30)
  )


### Tests ######################################################################

test_that("function completes with no errors", {
  expect_equal(nrow(strr_FREH(daily)), 40)
})

test_that("status warnings are given", {
  expect_warning(strr_FREH(daily, status_types = c("Q", "A")), "first supplied")
  expect_warning(strr_FREH(daily, status_types = c("R", "Q")),
                 "second supplied")
  expect_warning(strr_FREH(daily, entire_home = "TKTK"), "supplied")
})

test_that("incorrect fields are flagged", {
  expect_error(strr_FREH(daily, listing_type = flkj), "must be")
  expect_error(strr_FREH(daily, listing_type = TRUE), "must be")
})

test_that("listing_type == FALSE works", {
  expect_equal(nrow(strr_FREH(daily, listing_type = FALSE)), 40)
})

test_that("sf works", {
  expect_equal(nrow(strr_FREH(strr_as_sf(daily))), 40)
})

test_that("supplied dates work", {
  expect_equal(nrow(strr_FREH(daily, "2018-04-03")), 36)
  expect_equal(nrow(strr_FREH(daily, "2018-04-03", "2018-04-10")), 16)
  expect_error(strr_FREH(daily, "DSLKJ"), "start_date")
  expect_error(strr_FREH(daily, "2018-04-03", "DSLKJ"), "end_date")
})
