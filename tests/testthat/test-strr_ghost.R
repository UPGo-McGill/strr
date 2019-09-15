context("strr_ghost arguments")

library(tibble)

points <- tibble(
  property_ID = 1:15,
  host_ID = c(rep(c("4 to 3", "EH_check TRUE", "EH_check FALSE", "No GH"), 3),
              c("4 to 3", "EH_check TRUE", "EH_check FALSE")),
  created = c("2018-01-01", "2018-02-01", "2016-04-01", "2018-04-01",
              "2014-01-01", "2017-06-01", "2016-01-01", "2012-01-01",
              "2015-09-01", "2017-06-01", "2016-01-01", "2015-01-01",
              "2018-01-01", "2015-01-01", "2016-01-01"),
  scraped = c("2019-04-01", "2019-04-01", "2018-05-01", "2018-05-01",
              "2019-01-01", "2019-04-01", "2017-01-01", "2017-01-01",
              "2019-04-01", "2019-04-01", "2017-01-01", "2016-01-01",
              "2019-03-01", "2019-04-01", "2018-08-01"),
  listing_type = "Private room",
  geometry = st_sfc(st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
                    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
                    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
                    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
                    st_point(c(1, 1)), st_point(c(1, 1)), st_point(c(1, 1)),
                    crs = 32617)
) %>% st_as_sf() %>% arrange(host_ID)

test_that("cores/distance/min_listings flags are correctly handled", {
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

test_that("listing_type is correctly handled", {
  expect_error
})

