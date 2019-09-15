#' Function to compress raw daily STR tables into UPGo DB format
#'
#' \code{strr_compress_daily} takes raw monthyl daily tables from AirDNA and
#' compresses them into the UPGo database storage format.
#'
#' A function for compressing the daily activity tables supplied each month from
#' AirDNA into a more storage-efficient one-activity-block-per-row format. The
#' function also produces error files which identify possible corrupt or missing
#' lines in the input file.
#'
#' @param daily A daily table in the raw AirDNA format.
#' @param year-month A character string of the format "YYYY-MM" identifying the
#' year and month of the daily table.
#' @param cores A positive integer scalar. How many processing cores should be
#' used to perform the computationally intensive compression step?
#' @return The output will be a list with three elements: 1) the compressed
#' daily table, ready for upload to a remote database; 2) an error table
#' identifying corrupt or otherwise invalid row entries; 3) a missing_rows
#' table identifying property_IDs with missing dates in between their first and
#' last date entries, and therefore potentially missing data.
#' @importFrom data.table setDT
#' @importFrom dplyr %>% filter mutate pull select
#' @importFrom tibble as_tibble
#' @export

strr_compress_daily <- function(daily, output_date = NULL, cores = 1) {

  ## Error checking and initialization

  .datatable.aware = TRUE

  # Check that cores is an integer > 0
  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }

  # Check that output_date is in the required format
  if (!missing(output_date)) {
    if (!stringr::str_detect(
    output_date, "[:digit:][:digit:][:digit:][:digit:]_[:digit:][:digit:]")) {
      stop("The argument `output_date` must be a character string of form ",
           "'YYYY_MM'")
    }}

  ## Subset daily table and rename fields

  daily <-
    daily %>%
    select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
           `Reservation ID`) %>%
    rlang::set_names(c("property_ID", "date", "status", "booked_date", "price",
                "res_ID"))

  ## Find rows with readr errors and add to error file

  error <-
    problems(daily) %>%
    filter(expected != "10 columns", expected != "no trailing characters") %>%
    pull(row) %>%
    daily[.,]

  error_vector <-
    problems(daily) %>%
    filter(expected != "10 columns", expected != "no trailing characters") %>%
    pull(row)

  if (length(error_vector) > 0) {daily <- daily[-error_vector,]}

  ## Find rows with missing property_ID, date or status

  error <-
    daily %>%
    filter(is.na(property_ID) | is.na(date) | is.na(status)) %>%
    rbind(error)

  daily <-
    daily %>%
    filter(!is.na(property_ID), !is.na(date), !is.na(status))

  ## Check status

  error <-
    daily %>%
    filter(!(status %in% c("A", "U", "B", "R"))) %>%
    rbind(error)

  daily <-
    daily %>%
    filter(status %in% c("A", "U", "B", "R"))

  ## Remove duplicate listing entries by price, but don't add to error file

  daily <-
    daily %>%
    filter(!is.na(price))

  ## Find missing rows

  setDT(daily)
  missing_rows <-
    daily[, list(count = .N,
                 full_count = as.integer(max(date) - min(date) + 1)),
          by = property_ID]
  missing_rows <-
    missing_rows %>%
    as_tibble() %>%
    mutate(dif = full_count - count) %>%
    filter(dif != 0)

  ## Produce month and year columns to make unique entries

  daily <-
    daily %>%
    as_tibble() %>%
    mutate(month = month(date), year = year(date))

  ## Compress processed daily file

  if (cores > 1) {

    daily_list <- split(daily, daily$property_ID)

    daily_list <- map(1:1000, function(i) {
      rbindlist(
        daily_list[(floor(length(daily_list) * (i - 1) / 1000) +
                      1):floor(length(daily_list) * i / 1000)])

      daily <-
        daily_list %>%
        pbapply::pblapply(strr_compress_helper, cl = cores) %>%
        do.call(rbind, .)
    })
  } else daily <- strr_compress_helper(daily)


  ## Write files to disk if output_date is specified

  if (!missing(output_date)) {

    save(daily, file = paste0("output/", substr(output_date, 1, 4), "/daily_",
                              output_date, ".Rdata"))
    write_csv(error, paste0("output/", substr(output_date, 1, 4), "/error_",
                            output_date, ".csv"))
    write_csv(missing_rows, paste0("output/", substr(output_date, 1, 4),
                                   "/missing_rows_", output_date, ".csv"))

  }

  return(list(daily, error, missing_rows))
}


#' Helper function to compress daily file
#'
#' \code{strr_compress_helper} takes a processed `daily` table and generates a
#' compressed version.
#'
#' A helper function for compressing the processed monthly `daily` table.
#'
#' @param daily The processed daily table generated through the
#' strr_compress_daily function.
#' @return The output will be a compressed daily table.
#' @importFrom dplyr %>% arrange bind_rows filter group_by mutate select
#' @importFrom dplyr summarize ungroup
#' @importFrom purrr map
#' @importFrom tidyr unnest
#' @importFrom tibble tibble

strr_compress_helper <- function(daily) {
  daily <-
    daily %>%
    group_by(property_ID, status, booked_date, price, res_ID, month, year) %>%
    summarize(dates = list(date)) %>%
    ungroup()

  single_date <-
    daily %>%
    filter(map(dates, length) == 1) %>%
    mutate(start_date = as.Date(map_dbl(dates, ~{.x}), origin = "1970-01-01"),
           end_date = as.Date(map_dbl(dates, ~{.x}), origin = "1970-01-01")) %>%
    select(property_ID, start_date, end_date, status, booked_date, price,
           res_ID)

  one_length <-
    daily %>%
    filter(map(dates, length) != 1,
           map(dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
    mutate(start_date = as.Date(map_dbl(dates, min), origin = "1970-01-01"),
           end_date = as.Date(map_dbl(dates, max), origin = "1970-01-01")) %>%
    select(property_ID, start_date, end_date, status, booked_date, price,
           res_ID)

  remainder <-
    daily %>%
    filter(map(dates, length) != 1,
           map(dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
    mutate(date_range = map(dates, ~{
      tibble(start_date = .x[which(diff(c(0, .x)) > 1)],
             end_date = .x[which(diff(c(.x, 30000)) > 1)])
    })) %>%
    unnest(date_range) %>%
    select(property_ID, start_date, end_date, status, booked_date, price,
           res_ID)

  bind_rows(single_date, one_length, remainder) %>%
    arrange(property_ID, start_date)
}
