#### TESTS FOR strr_multi ######################################################

### Setup ######################################################################

context("strr_multi tests")

daily_multi <-
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
                host_ID = rep("host-1", 30),
                listing_type = "Entire home/apt",
                housing = NA,
                country = "Canada",
                region = c(rep("Ontario", 20), rep("Quebec", 10)),
                city = NA)

host_multi <- strr_host(daily_multi, quiet = TRUE)


### Tests ######################################################################

test_that("function completes with no errors", {
  expect_equal(sum((
    strr_multi(daily_multi, host_multi)
    )$multi), 20)
})

test_that("function completes with unnamed thresholds and SR/HR > 0", {
  expect_equal(sum((
    strr_multi(daily_multi, host_multi, c(2, 3, 1, 1))
  )$multi), 20)
})

