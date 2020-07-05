#### TESTS FOR strr_compress ###################################################

### Setup ######################################################################

context("strr_compress tests")

daily <-
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
         booked_date = as.Date(NA, origin = "1970-01-01"),
         price = rep(100, 30),
         res_ID = NA,
         host_ID = c(rep("host-1", 20), rep("host-2", 10)),
         listing_type = NA,
         housing = NA,
         country = "Canada",
         region = c(rep("Ontario", 20), rep("Quebec", 10)),
         city = NA)

daily_2 <-
  dplyr::tibble(property_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
                date = as.Date(c(
                  "2018-12-21", "2018-12-22", "2018-12-23", "2018-12-24",
                  "2018-12-25", "2018-12-26", "2018-12-27", "2018-12-28",
                  "2018-12-29", "2018-12-30", "2018-12-31", "2019-01-01",
                  "2019-01-02", "2019-01-03", "2019-01-04", "2019-01-05",
                  "2019-01-06", "2019-01-07", "2019-01-08", "2019-01-09",
                  "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
                  "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
                  "2018-04-09", "2018-04-10")),
                status = c(
                  rep("A", 5), rep("R", 2), rep("A", 13),
                  rep("A", 4), rep("B", 3), rep("R", 3)),
                booked_date = as.Date(NA, origin = "1970-01-01"),
                price = rep(100, 30),
                res_ID = NA,
                host_ID = c(rep("host-1", 20), rep("host-2", 10)),
                listing_type = NA,
                housing = NA,
                country = "Canada",
                region = c(rep("Ontario", 20), rep("Quebec", 10)),
                city = NA)

daily_3 <-
  dplyr::tibble(property_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
                date = as.Date(c(
                  "2018-01-21", "2018-01-22", "2018-01-23", "2018-01-24",
                  "2018-01-25", "2018-01-26", "2018-01-27", "2018-01-28",
                  "2018-01-29", "2018-01-30", "2018-01-31", "2018-02-01",
                  "2018-02-02", "2018-02-03", "2018-02-04", "2018-02-05",
                  "2018-02-06", "2018-02-07", "2018-02-08", "2018-02-09",
                  "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
                  "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
                  "2018-04-09", "2018-04-10")),
                status = c(
                  rep("A", 5), rep("R", 2), rep("A", 13),
                  rep("A", 4), rep("B", 3), rep("R", 3)),
                booked_date = as.Date(NA, origin = "1970-01-01"),
                price = rep(100, 30),
                res_ID = NA,
                host_ID = c(rep("host-1", 20), rep("host-2", 10)),
                listing_type = NA,
                housing = NA,
                country = "Canada",
                region = c(rep("Ontario", 20), rep("Quebec", 10)),
                city = NA)

host <-
  dplyr::tibble(host_ID = c(rep("ab-1", 20), rep("ab-2", 10)),
         date = as.Date(c(
           "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
           "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
           "2018-04-09", "2018-04-10", "2018-04-11", "2018-04-12",
           "2018-04-13", "2018-04-14", "2018-04-15", "2018-04-16",
           "2018-04-17", "2018-04-18", "2018-04-19", "2018-04-20",
           "2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04",
           "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-08",
           "2018-04-09", "2018-04-10")),
         listing_type = c(
           rep("A", 5), rep("R", 2), rep("A", 13),
           rep("A", 4), rep("B", 3), rep("R", 3)),
         housing = NA,
         count = c(rep(1, 10), rep(3, 20)))







strr_compress_test <- function(data, quiet = FALSE) {

  ### ERROR CHECKING AND INITIALIZATION ########################################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  daily <- helper_check_data()

  helper_check_quiet()

  if (daily) {helper_message("Daily table identified.")
  } else helper_message("Host table identified.")


  ## Silence R CMD check for data.table fields ---------------------------------

  property_ID <- month <- PID_split <- host_split <- host_ID <- start_date <-
    NULL


  ### PREPARE FILE FOR ANALYSIS ################################################

  ## Convert to data.table -----------------------------------------------------

  data.table::setDT(data)


  ## Store invariant fields for later ------------------------------------------

  if (daily) {

    join_cols <-
      c("host_ID", "listing_type", "housing", "country", "region", "city")

    join_fields <- data[, .SD[1L], by = property_ID, .SDcols = join_cols]

    data[, (join_cols) := NULL]

  }


  ## If data spans multiple months, produce month/year columns -----------------

  steps <- 2

  if (data.table::year(min(data$date)) != data.table::year(max(data$date))) {

    steps <- 3

    helper_message("(1/", steps, ") Adding year and month fields.",
                   .type = "open")

    data[, c("month", "year") := list(data.table::month(date),
                                      data.table::year(date))]

    helper_message("(1/", steps, ") Year and month fields added.",
                   .type = "close")

  } else if (data.table::month(min(data$date)) !=
             data.table::month(max(data$date))) {

    steps <- 3

    helper_message("(1/", steps, ") Adding month field.", .type = "open")

    data[, month := data.table::month(date)]

    helper_message("(1/", steps, ") Month field added.", .type = "close")

  }


  ### SPLIT TABLE FOR PROCESSING ###############################################

  ## Split by first three digits of property_ID/host_ID ------------------------

  if (daily) {

    data[, PID_split := substr(property_ID, 1, 6)]

    data_list <- split(data, by = "PID_split", keep.by = FALSE)
    data_list <- helper_table_split(data_list)

  } else {

    data[, host_split := substr(host_ID, 1, 3)]

    data_list <- split(data, by = "host_split", keep.by = FALSE)
    data_list <- helper_table_split(data_list)
  }


  ### COMPRESS PROCESSED DATA FILE #############################################

  helper_message("(", steps - 1, "/", steps,
                 ") Compressing rows, using {helper_plan()}.")

  handler_strr("Compressing row")

  with_progress2({

    .strr_env$pb <- progressor2(steps = nrow(data))

    if (daily)  compressed <- par_lapply(data_list, helper_compress_daily)
    if (!daily) compressed <- par_lapply(data_list, helper_compress_host)

  })
  #
  #
  # ## Rbind and add other columns -----------------------------------------------
  #
  # compressed <- data.table::rbindlist(compressed)
  #
  # # The join is faster and less memory-intensive with dplyr than data.table
  # if (daily) compressed <-
  #   dplyr::left_join(compressed, join_fields, by = "property_ID")
  #
  #
  # ### ARRANGE OUTPUT AND SET CLASS #############################################
  #
  # helper_message("(", steps, "/", steps, ") Arranging output table.",
  #                .type = "open")
  #
  # data.table::setDT(compressed)
  #
  # if (daily) {
  #
  #   compressed <- dplyr::as_tibble(compressed[order(property_ID, start_date)])
  #
  # } else {
  #
  #   compressed <- dplyr::as_tibble(compressed[order(host_ID, start_date)])
  #
  # }
  #
  # helper_message("(", steps, "/", steps, ") Output table arranged.",
  #                .type = "close")
  #
  #
  # ### RETURN OUTPUT ############################################################
  #
  # helper_message("Compression complete.", .type = "final")

  return(compressed)
}





### Tests ######################################################################

# test_that("helper functions work", {
#
#   expect_equal(nrow(helper_compress_daily(daily)), 6)
#   expect_equal(nrow(helper_compress_host(host)), 7)
#
# })

# test_that("first X lines complete", {
#
#   expect_equal(length(strr_compress_test(daily)), 2)
#
# })
#
#
# test_that("function completes with no errors", {
#   # Daily file
#   expect_equal(nrow(strr_compress(daily)), 6)
#   # Host file
#   expect_equal(nrow(strr_compress(host)), 7)
# })
#
# test_that("function sets correct start/end dates", {
#   # Start date
#   expect_equal(pull(strr_compress(daily), start_date)[2], as.Date("2018-04-06"))
#   # End date
#   expect_equal(pull(strr_compress(daily), end_date)[2], as.Date("2018-04-07"))
# })
#
# test_that("month/year boundaries are handled properly", {
#   expect_equal(nrow(strr_compress(daily_2)), 7)
#   expect_equal(nrow(strr_compress(daily_3)), 7)
# })
#
# test_that("one_length and remainder conditions are handled properly", {
#   expect_equal(nrow(strr_compress(daily[c(1:5, 8:10),])), 2)
#   expect_equal(nrow(strr_compress(host[c(1:5, 8:10),])), 2)
#   expect_equal(nrow(strr_compress(host[c(1:5),])), 1)
# })
#
# test_that("The quiet flag suppresses all messages", {
#   expect_message(strr_compress(daily, quiet = TRUE), regexp = NA)
# })
