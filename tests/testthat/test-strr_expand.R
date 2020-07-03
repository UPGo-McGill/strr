#### TESTS FOR strr_expand #####################################################

### Setup ######################################################################

context("strr_expand tests")

daily <-
  dplyr::tibble(
    property_ID = c(rep("ab-1", 3), rep("ab-2", 3)),
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

daily_enormous <-
  dplyr::tibble(
    property_ID = c(rep("ab-1", 3), rep("ab-2", 3)),
    start_date = as.Date(c(
      "2001-04-01", "2001-04-06", "2001-04-08",
      "2001-04-01", "2001-04-05", "2001-04-08")),
    end_date = as.Date(c(
      "2218-04-05", "2218-04-07", "2218-04-20",
      "2218-04-04", "2218-04-07", "2218-04-10")),
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

host_expand <-
  dplyr::tibble(
    host_ID = c("10000029", "10000029", "10000029", "1000008", "1000008",
                "1000008", "1000014", "1000014", "1000014", "1000014"),
    start_date = as.Date(c("2014-10-01", "2014-11-01", "2014-12-01",
                           "2014-10-01", "2014-11-01", "2014-12-01",
                           "2014-10-01", "2014-10-03", "2014-10-07",
                           "2014-10-16")),
    end_date = as.Date(c("2014-10-31", "2014-11-30", "2014-12-31", "2014-10-31",
                         "2014-11-30", "2014-12-31", "2014-10-01", "2014-10-05",
                         "2014-10-14", "2014-10-31")),
    listing_type = "Entire home/apt",
    housing = TRUE,
    count = 1
  )


### Tests ######################################################################

test_that("function succeeds with no errors", {
  # Daily
  expect_equal(nrow(strr_expand(daily)), 30)
  # Host
  expect_equal(nrow(strr_expand(host_expand)), 212)
})

test_that("the quiet flag suppresses all messages", {
  expect_message(strr_expand(daily, quiet = TRUE), regexp = NA)
})

test_that("batches work", {
  skip_if_not(isTRUE(as.logical(Sys.getenv("CI"))) | run_all_tests)
  expect_equal(nrow(
    strr_expand(
      data.table::rbindlist(replicate(1800000, daily, simplify = FALSE))
      )
    ), 54000000)
  expect_equal(nrow(
    strr_expand(
      data.table::rbindlist(replicate(1800000, host_expand, simplify = FALSE))
    )
  ), 381600000)

})

test_that("enormous tables are flagged", {
  skip_if_not(isTRUE(as.logical(Sys.getenv("CI"))) | run_all_tests)
  expect_error(
    strr_expand(
      data.table::rbindlist(replicate(50000001, daily_enormous,
                                      simplify = FALSE))
      )
  )
})
