# #### TESTS FOR strr helpers ####################################################
#
# ### Setup ######################################################################
#
# context("strr_helpers tests")
#
# future::plan(multiprocess, workers = 4)
#
# ### Tests ######################################################################
#
# test_that("helper_table_split correctly exists its while-loop", {
#   # Initial multiplier is ok
#   map(1:1000, ~{tibble(id = .x, value = 1)}) %>%
#     helper_table_split() %>%
#     length() %>%
#     expect_equal(16)
#   # Initial multiplier is too high
#   map(1:25, ~{tibble(id = .x, value = 1)}) %>%
#     helper_table_split(10) %>%
#     length() %>%
#     expect_equal(24)
#   # expect_equal(nrow(strr_compress(multi)), 7)
#   # All multipliers are too high
# })
