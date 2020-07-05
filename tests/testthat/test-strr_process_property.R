#### TESTS FOR strr_process_property ###########################################

### Setup ######################################################################

context("strr_process_property tests")

load("strr_process_property_data.Rdata")


### Tests ######################################################################

test_that("The function successfully completes", {
  expect_equal(sum(sapply(strr_process_property(property_raw), nrow)), 50)
})
