#### TESTS FOR strr_expand #####################################################

### Setup ######################################################################

context("strr_expand tests")

daily <-
  tibble(property_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
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
         listing_type = NA,
         housing = NA,
         country = NA,
         region = NA,
         city = NA) %>%
  strr_compress()


### Tests ######################################################################

test_that("function succeeds with no errors", {
  # Basic test
  expect_equal(nrow(strr_expand(daily)), 30)
})

test_that("The quiet flag suppresses all messages", {
  expect_message(strr_expand(daily, quiet = TRUE), regexp = NA)
})