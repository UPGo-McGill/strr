#### TESTS FOR strr_compress ###################################################

### Setup ######################################################################

context("strr_compress tests")

daily_compress <-
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
         booked_date = as.Date(NA, origin = "1970-01-01"),
         price = rep(100, 30),
         res_ID = NA,
         host_ID = c(rep("host-1", 20), rep("host-2", 10)),
         listing_type = NA,
         housing = NA,
         country = "Canada",
         region = c(rep("Ontario", 20), rep("Quebec", 10)),
         city = NA)

daily_compress_2 <-
  dplyr::tibble(property_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
                date = as.Date(c(
                  "2018-12-21", "2018-12-22", "2018-12-23", "2018-12-24",
                  "2018-12-25", "2018-12-26", "2018-12-27", "2018-12-28",
                  "2018-12-29", "2018-12-30", "2018-12-31", "2019-01-01",
                  "2019-01-02", "2019-01-03", "2019-01-04", "2019-01-05",
                  "2019-01-06", "2019-01-07", "2019-01-08", "2019-01-09",
                  "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
                  "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
                  "2018-04-09", "2018-04-10")),
                status = c(
                  rep("A", 5), rep("R", 2), rep("A", 13),
                  rep("A", 4), rep("B", 3), rep("R", 3)),
                booked_date = as.Date(NA, origin = "1970-01-01"),
                price = rep(100, 30),
                res_ID = NA,
                host_ID = c(rep("host-1", 20), rep("host-2", 10)),
                listing_type = NA,
                housing = NA,
                country = "Canada",
                region = c(rep("Ontario", 20), rep("Quebec", 10)),
                city = NA)

daily_compress_3 <-
  dplyr::tibble(property_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
                date = as.Date(c(
                  "2018-01-21", "2018-01-22", "2018-01-23", "2018-01-24",
                  "2018-01-25", "2018-01-26", "2018-01-27", "2018-01-28",
                  "2018-01-29", "2018-01-30", "2018-01-31", "2018-02-01",
                  "2018-02-02", "2018-02-03", "2018-02-04", "2018-02-05",
                  "2018-02-06", "2018-02-07", "2018-02-08", "2018-02-09",
                  "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
                  "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
                  "2018-04-09", "2018-04-10")),
                status = c(
                  rep("A", 5), rep("R", 2), rep("A", 13),
                  rep("A", 4), rep("B", 3), rep("R", 3)),
                booked_date = as.Date(NA, origin = "1970-01-01"),
                price = rep(100, 30),
                res_ID = NA,
                host_ID = c(rep("host-1", 20), rep("host-2", 10)),
                listing_type = NA,
                housing = NA,
                country = "Canada",
                region = c(rep("Ontario", 20), rep("Quebec", 10)),
                city = NA)

host_compress <-
  dplyr::tibble(host_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
         date = as.Date(c(
           "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
           "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
           "2018-04-09", "2018-04-10", "2018-04-11", "2018-04-12",
           "2018-04-13", "2018-04-14", "2018-04-15", "2018-04-16",
           "2018-04-17", "2018-04-18", "2018-04-19", "2018-04-20",
           "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
           "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
           "2018-04-09", "2018-04-10")),
         listing_type = c(
           rep("A", 5), rep("R", 2), rep("A", 13),
           rep("A", 4), rep("B", 3), rep("R", 3)),
         housing = NA,
         count = c(rep(1, 10), rep(3, 20)))


### Tests ######################################################################

test_that("helper functions work", {

  expect_equal(nrow(helper_compress_daily(daily_compress)), 6)
  expect_equal(nrow(helper_compress_host(host_compress)), 7)

})

test_that("function completes with no errors", {
  # Daily file
  expect_equal(nrow(strr_compress(daily_compress)), 6)
  # Host file
  expect_equal(nrow(strr_compress(host_compress)), 7)
})

test_that("function sets correct start/end dates", {
  # Start date
  expect_equal(dplyr::pull(strr_compress(daily_compress), start_date)[2],
               as.Date("2018-04-06"))
  # End date
  expect_equal(dplyr::pull(strr_compress(daily_compress), end_date)[2],
               as.Date("2018-04-07"))
})

test_that("month/year boundaries are handled properly", {
  expect_equal(nrow(strr_compress(daily_compress_2)), 7)
  expect_equal(nrow(strr_compress(daily_compress_3)), 7)
})

test_that("one_length and remainder conditions are handled properly", {
  expect_equal(nrow(strr_compress(daily_compress[c(1:5, 8:10),])), 2)
  expect_equal(nrow(strr_compress(host_compress[c(1:5, 8:10),])), 2)
  expect_equal(nrow(strr_compress(host_compress[c(1:5),])), 1)
})

test_that("The quiet flag suppresses all messages", {
  expect_message(strr_compress(daily_compress, quiet = TRUE), regexp = NA)
})
