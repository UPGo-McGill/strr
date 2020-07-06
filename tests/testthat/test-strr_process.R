#### TESTS FOR strr_process_* FUNCTIONS ########################################

### Setup ######################################################################

context("strr_process_* tests")

load("strr_process_property_data.Rdata")
load("strr_process_daily_data.Rdata")
load("strr_process_review_data.Rdata")

property_list <- strr_process_property(property_raw)


### Tests ######################################################################

test_that("strr_process_property successfully completes", {
  expect_equal(sum(sapply(property_list, nrow)), 50)
})

test_that("strr_process_daily successfully completes", {
  expect_equal(sum(sapply(
    strr_process_daily(daily_raw, property_list[[1]]), nrow)), 28712)
})

test_that("strr_process_review successfully completes", {
  expect_equal(sum(sapply(
    strr_process_review(review_raw, property_list[[1]], latest_user),
    nrow)),
    3435)
})

test_that("strr_process_review works with latest_user missing", {
  expect_equal(sum(sapply(
    strr_process_review(review_raw, property_list[[1]]), nrow)),
    3998)
})
