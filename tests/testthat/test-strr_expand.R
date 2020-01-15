#### TESTS FOR strr_expand #####################################################

### Setup ######################################################################

context("strr_expand tests")

daily <-
  tibble(property_ID = c(rep("ab-1", 3), rep("ab-2", 3)),
         start_date = as.Date(c(
           "2018-04-01", "2018-04-06", "2018-04-08",
           "2018-04-01", "2018-04-05", "2018-04-08")),
         end_date = as.Date(c(
           "2018-04-05", "2018-04-07", "2018-04-20",
           "2018-04-04", "2018-04-07", "2018-04-10")),
         status = c("A", "R", "A", "A", "B", "R"),
         booked_date = NA,
         price = c(100, 100, 50, 40, 100, 60),
         res_ID = NA,
         host_ID = c("A", "A", "A", "B", "B", "B"),
         listing_type = "Entire home/apt",
         housing = TRUE,
         country = "Canada",
         region = "Québec",
         city = c("Montreal", "Montreal", "Montreal", "Laval", "Laval", "Laval")
         )

class(daily) <- append(class(daily), "strr_daily")


### Tests ######################################################################

test_that("function succeeds with no errors", {
  # Basic test
  expect_equal(nrow(strr_expand(daily)), 30)
})

test_that("The quiet flag suppresses all messages", {
  expect_message(strr_expand(daily, quiet = TRUE), regexp = NA)
})
