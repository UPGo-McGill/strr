context("strr_ghost tests")

#' @importFrom dplyr %>% arrange
#' @importFrom tibble tibble
#' @importFrom sf st_as_sf st_drop_geometry st_point

points <- tibble::tibble(
  property_ID = 1:19,
  host_ID = c(
    rep("No GH", 3),
    rep("4 to 3", 4),
    rep("listing_type", 4),
    rep("EH_check TRUE", 4),
    rep("EH_check FALSE", 4)),
  created = as.Date(c(
    "2018-04-01", "2012-01-01", "2015-01-01",
    "2018-01-01", "2014-01-01", "2015-09-01", "2018-01-01",
    "2018-01-01", "2018-01-01", "2018-01-01", "2018-01-01",
    "2018-02-01", "2017-06-01", "2017-06-01", "2015-01-01",
    "2016-04-01", "2016-01-01", "2016-01-01", "2016-01-01"
    )),
  scraped = as.Date(c(
    "2018-05-01", "2017-01-01", "2016-01-01",
    "2019-04-01", "2019-01-01", "2019-04-01", "2019-03-01",
    "2019-04-01", "2019-04-01", "2019-04-01", "2019-04-01",
    "2019-04-01", "2019-04-01", "2019-04-01", "2019-04-01",
    "2018-05-01", "2017-01-01", "2017-01-01", "2018-08-01"
  )),
  listing_type = c(
    rep("Private room", 3),
    rep("Private room", 4),
    rep("Private room", 3), "Entire home/apt",
    rep("Private room", 3), "Entire home/apt",
    rep("Private room", 3), "Entire home/apt"
    ),
  geometry = st_sfc(
    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
    crs = 32617)
  ) %>% st_as_sf()

test_that("cores/distance/min_listings flags are correctly handled", {
  # cores
  expect_error(strr_ghost(points, property_ID, host_ID, multi_date = TRUE,
                          created, scraped, distance = 200, min_listings = 3,
                          listing_type = listing_type,
                          private_room = "Private room", cores = -1))
  # distance
  expect_error(strr_ghost(points, property_ID, host_ID, multi_date = TRUE,
                          created, scraped, distance = -200, min_listings = 3,
                          listing_type = listing_type,
                          private_room = "Private room", cores = 1))
  # min_listings
  expect_error(strr_ghost(points, property_ID, host_ID, multi_date = TRUE,
                          created, scraped, distance = 200, min_listings = -3,
                          listing_type = listing_type,
                          private_room = "Private room", cores = 1))
})

test_that("handling of sf/sp classes is correct", {
  # No sf or sp
  expect_error(strr_ghost(st_drop_geometry(points)))
  # Convert sp
  ### TEST FOR sp CONVERSION TKTK
  # CRS handling
  ### TEST FOR CRS HANDLING? TKTK

})


test_that("points fields are correctly handled", {
  # property_ID
  expect_error(strr_ghost(points, property_ID = missing_field),
               "`property_ID` is not")
  # host_ID
  expect_error(strr_ghost(points, host_ID = missing_field),
               "`host_ID` is not")
  # created
  expect_error(strr_ghost(points, created = missing_field),
               "`created` is not")
  # scraped
  expect_error(strr_ghost(points, scraped = missing_field),
               "`scraped` is not")
  # listing_type missing
  expect_error(strr_ghost(points, listing_type = missing_field),
               "`listing_type` must be")
  # listing_type TRUE
  expect_error(strr_ghost(points, listing_type = TRUE),
               "`listing_type` must be")
})

test_that("listing_type is correctly handled", {
  # listing_type = listing_type
  expect_equal({
    strr_ghost(points, quiet = TRUE) %>%
      filter(host_ID == "listing_type", date == "2019-04-01") %>%
      pull(listing_count)
  }, 3)

  # listing_type = FALSE
  expect_equal({
    strr_ghost(points, listing_type = FALSE, quiet = TRUE) %>%
      filter(host_ID == "listing_type", date == "2019-04-01") %>%
      pull(listing_count)
    }, 4)
})

test_that("multi_date produces the expected outputs", {
  expect_equal({
    strr_ghost(points, quiet = TRUE) %>%
      filter(date == "2016-01-01", host_ID == "4 to 3") %>%
      nrow()}, 0)

  expect_equal({
    strr_ghost(points, quiet = TRUE) %>%
      filter(date == "2018-01-01", host_ID == "4 to 3") %>%
      pull(listing_count)}, 4)

  expect_equal({
    strr_ghost(points, quiet = TRUE) %>%
      filter(date == "2019-03-01", host_ID == "4 to 3") %>%
      pull(listing_count)}, 3)

  expect_equal({
    strr_ghost(points, quiet = TRUE) %>%
      filter(date == "2019-04-01", host_ID == "4 to 3") %>%
      nrow()}, 0)

  expect_equal({
    strr_ghost(points, multi_date = FALSE, quiet = TRUE) %>%
      filter(host_ID == "4 to 3") %>%
      pull(listing_count)}, 4)
})

# test_that("EH_check works correctly", {
#
# })
