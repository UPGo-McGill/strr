property <-
  dplyr::tibble(
    property_ID = c("a", "b", "c"),
    host_ID = c("1", "2", "3"),
    longitude = c(76, 72, 71),
    latitude = c(-34, -35, -36)
    )

test_that("the function works", {
  expect_equal(length(strr_as_sf(property)), 3)
  expect_s3_class(strr_as_sf(property), "sf")
})
