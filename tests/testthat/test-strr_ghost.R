context("strr_ghost arguments")

library(tibble)

points <- tibble(
  property_ID = 1:12,
  host_ID = rep(1:4, 3),
  created = c("2018-01-01", "2018-02-01", "2018-04-01", "2014-01-01",
              "2014-01-01", "2017-06-01", "2012-01-01", "2019-03-01",
              "2015-09-01", "2017-06-01", "2015-01-01", "2014-01-01"),
  scraped = c("2019-04-01", "2019-04-01", "2018-05-01", "2019-04-01",
              "2019-01-01", "2019-04-01", "2017-01-01", "2019-04-01",
              "2019-04-01", "2019-04-01", "2016-01-01", "2019-04-01"),
  listing_type = "Private room"
)

test_that("cores, distance and min_listings flags are correctly handled", {
  expect_error(strr_ghost(points, property_ID, host_ID, multi_date = TRUE,
                          created, scraped, distance = 200, min_listings = 3,
                          listing_type = listing_type,
                          private_room = "Private room", cores = -1))
  expect_error(strr_ghost(points, property_ID, host_ID, multi_date = TRUE,
                          created, scraped, distance = -200, min_listings = 3,
                          listing_type = listing_type,
                          private_room = "Private room", cores = 1))
  expect_error(strr_ghost(points, property_ID, host_ID, multi_date = TRUE,
                          created, scraped, distance = 200, min_listings = -3,
                          listing_type = listing_type,
                          private_room = "Private room", cores = 1))
})
