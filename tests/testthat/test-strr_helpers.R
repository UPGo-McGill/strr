#### TESTS FOR strr helpers ####################################################

### Setup ######################################################################

library(dplyr)

context("strr_helpers tests")

data <-
  dplyr::tibble(
    a = c(rep("A", 200), rep("B", 50), rep("C", 50), rep("D", 50), rep("E", 25),
          rep("F", 25), rep("G", 10), rep("H", 10), rep("I", 8), rep("J", 5),
          rep("K", 79), rep("L", 39), rep("M", 10), rep("N", 99), rep("O", 39),
          rep("P", 211), rep("Q", 2), rep("R", 4), rep("S", 16), rep("T", 29),
          rep("U", 11), rep("V", 2), rep("W", 17), rep("X", 3), rep("Y", 5),
          rep("Z", 1)),
    b = 1:1000
)

data_list <-
  data %>%
  dplyr::group_split(a)

data_sf <-
  dplyr::tibble(
    a = c(rep("A", 200), rep("B", 50), rep("C", 50), rep("D", 50), rep("E", 25),
          rep("F", 25), rep("G", 10), rep("H", 10), rep("I", 8), rep("J", 5),
          rep("K", 79), rep("L", 39), rep("M", 10), rep("N", 99), rep("O", 39),
          rep("P", 211), rep("Q", 2), rep("R", 4), rep("S", 16), rep("T", 29),
          rep("U", 11), rep("V", 2), rep("W", 17), rep("X", 3), rep("Y", 5),
          rep("Z", 1)),
    b = 1:1000,
    lon = 1:1000,
    lat = 1001:2000
  ) %>% sf::st_as_sf(coords = c("lon", "lat"))

data_list_sf <-
  data_sf %>%
  group_split(a)


typeof(data_sf)

### Tests ######################################################################

test_that("helper_table_split produces the right number of elements", {

  expect_equal(length(helper_table_split(data_list)), 10)
  expect_equal(length(helper_table_split(data_list, 6)), 6)

})

test_that("helper_table_split works with sf tables", {

  expect_s3_class(helper_table_split(data_list_sf)[[1]], "sf")

})



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
