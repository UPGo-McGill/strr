#' Function to compress daily STR tables into UPGo DB format
#'
#' \code{strr_compress} takes daily tables (either monthly daily tables from
#' AirDNA or ML summary tables produced by UPGo) and compresses them into the
#' UPGo database storage format.
#'
#' A function for compressing daily activity tables (either raw daily tables
#' supplied each month from AirDNA or ML summary tables produced by UPGo) into a
#' more storage-efficient one-activity-block-per-row format. In the case of the
#' monthly AirDNA files, the function also produces error files which identify
#' possible corrupt or missing lines in the input file.
#'
#' @param .data An unprocessed daily table in either the raw AirDNA format or
#' the UPGo ML summary table format.
#' @param cores A positive integer scalar. How many processing cores should be
#' used to perform the computationally intensive compression step?
#' @param quiet A logical vector. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will depend on the input. In the case where the input is
#' a raw AirDNA daily table, the output will be a list with three elements: 1)
#' the compressed daily table, ready for upload to a remote database; 2) an
#' error table identifying corrupt or otherwise invalid row entries; 3) a
#' missing_rows table identifying property_IDs with missing dates in between
#' their first and last date entries, and therefore potentially missing data. In
#' the case where the input is an UPGo ML summary table, the output will be the
#' compressed ML table, ready for upload to a remote database.
#' @importFrom data.table setDT
#' @importFrom dplyr %>% bind_rows filter mutate pull select
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_compress <- function(.data, cores = 1, chunks = 10000, quiet = FALSE) {

  ## Error checking and initialization

  .datatable.aware = TRUE

  # Check that cores is an integer > 0
  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }

  ## Perform simple compression if the input is an ML table

  if (length(.data) == 4) {

    .data <-
      .data %>%
      mutate(month = lubridate::month(.data$date),
             year = lubridate::year(.data$date))

    if (cores > 1) {

      daily_list <- split(.data, .data$host_ID)

      daily_list <- purrr::map(1:100, function(i) {
        bind_rows(
          daily_list[(floor(length(daily_list) * (i - 1) / 100) +
                        1):floor(length(daily_list) * i / 100)])
      })

      compressed <-
        daily_list %>%
        pbapply::pblapply(strr_compress_helper_ML, cl = cores) %>%
        do.call(rbind, .)

    } else compressed <- strr_compress_helper_ML(.data)


     return(compressed)
  }



  ## Subset .data table and rename fields

  .data <-
    .data %>%
    select(.data$`Property ID`, .data$Date, .data$Status, .data$`Booked Date`,
           .data$`Price (USD)`, .data$`Reservation ID`) %>%
    rlang::set_names(c("property_ID", "date", "status", "booked_date", "price",
                "res_ID"))

  ## Find rows with readr errors and add to error file

  if (!quiet) {message("Beginning error check.")}

  error <-
    readr::problems(.data) %>%
    filter(.data$expected != "10 columns",
           .data$expected != "no trailing characters") %>%
    pull(row) %>%
    .data[.,]

  error_vector <-
    readr::problems(.data) %>%
    filter(.data$expected != "10 columns",
           .data$expected != "no trailing characters") %>%
    pull(row)

  if (length(error_vector) > 0) {.data <- .data[-error_vector,]}

  if (!quiet) {message("Initial import errors identified.")}

  ## Find rows with missing property_ID, date or status

  error <-
    .data %>%
    filter(is.na(.data$property_ID) | is.na(.data$date) |
             is.na(.data$status)) %>%
    rbind(error)

  .data <-
    .data %>%
    filter(!is.na(.data$property_ID), !is.na(.data$date), !is.na(.data$status))

  if (!quiet) {
    message("Rows with missing property_ID, date or status identified.")}

  ## Check status

  error <-
    .data %>%
    filter(!(.data$status %in% c("A", "U", "B", "R"))) %>%
    rbind(error)

  .data <-
    .data %>%
    filter(.data$status %in% c("A", "U", "B", "R"))

  if (!quiet) {message("Rows with invalid status identified.")}

  ## Remove duplicate listing entries by price, but don't add to error file

  .data <-
    .data %>%
    filter(!is.na(.data$price))

  if (!quiet) {message("Duplicate rows removed.")}

  ## Find missing rows

  .N = property_ID = NULL # due to NSE notes in R CMD check

  setDT(.data)
  missing_rows <-
    .data[, .(count = .N,
                 full_count = as.integer(max(date) - min(date) + 1)),
          by = property_ID]

  missing_rows <-
    missing_rows %>%
    as_tibble() %>%
    mutate(dif = .data$full_count - .data$count) %>%
    filter(.data$dif != 0)

  if (!quiet) {message("Missing rows identified.")}

  ## Produce month and year columns to make unique entries

  .data <-
    .data %>%
    as_tibble() %>%
    mutate(month = lubridate::month(.data$date),
           year = lubridate::year(.data$date))

  ## Compress processed .data file

  if (!quiet) {message("Error check complete. Beginning compression.")}

  if (cores > 1) {

    daily_list <- split(.data, .data$property_ID)

    if (length(daily_list) > chunks) {
      daily_list <- purrr::map(1:chunks, function(i) {
        bind_rows(
          daily_list[(floor(as.numeric(length(daily_list)) * (i - 1) / chunks) +
                        1):floor(as.numeric(length(daily_list)) * i / chunks)])
      })}

    compressed <-
      daily_list %>%
      pbapply::pblapply(strr_compress_helper, cl = cores) %>%
      do.call(rbind, .)

  } else compressed <- strr_compress_helper(.data)

  return(list(compressed, error, missing_rows))
}


#' Helper function to compress daily file
#'
#' \code{strr_compress_helper} takes a processed `daily` table and generates a
#' compressed version.
#'
#' A helper function for compressing the processed monthly `daily` table.
#'
#' @param .data The processed daily table generated through the strr_compress
#' function.
#' @return The output will be a compressed daily table.
#' @importFrom dplyr %>% arrange bind_rows filter group_by mutate select
#' @importFrom dplyr summarize ungroup
#' @importFrom purrr map map_dbl
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @importFrom tibble tibble

strr_compress_helper <- function(.data) {
  .data <-
    .data %>%
    group_by(.data$property_ID, .data$status, .data$booked_date, .data$price,
             .data$res_ID, .data$month, .data$year) %>%
    summarize(dates = list(.data$date)) %>%
    ungroup()

  single_date <-
    .data %>%
    filter(map(.data$dates, length) == 1) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, ~{.x}),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, ~{.x}),
                              origin = "1970-01-01")) %>%
    select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
           .data$booked_date, .data$price, .data$res_ID)

  one_length <-
    .data %>%
    filter(map(.data$dates, length) != 1,
           map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, min),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, max),
                              origin = "1970-01-01")) %>%
    select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
           .data$booked_date, .data$price, .data$res_ID)

  if ({.data %>%
      filter(map(.data$dates, length) != 1,
             map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
      nrow} > 0) {

    remainder <-
      .data %>%
      filter(map(.data$dates, length) != 1,
             map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
      mutate(date_range = map(.data$dates, ~{
        tibble(start_date = .x[which(diff(c(0, .x)) > 1)],
               end_date = .x[which(diff(c(.x, 30000)) > 1)])
      })) %>%
      unnest(.data$date_range) %>%
      select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
             .data$booked_date, .data$price, .data$res_ID)

  } else remainder <- single_date[0,]

  bind_rows(single_date, one_length, remainder) %>%
    arrange(.data$property_ID, .data$start_date)
}


#' Helper function to compress daily file
#'
#' \code{strr_compress_helper_ML} takes a processed ML table and generates a
#' compressed version.
#'
#' A helper function for compressing the processed ML summary table.
#'
#' @param .data The processed ML table generated through the strr_compress
#' function.
#' @return The output will be a compressed ML table.
#' @importFrom dplyr %>% arrange bind_rows filter group_by mutate select
#' @importFrom dplyr summarize ungroup
#' @importFrom purrr map map_dbl
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @importFrom tibble tibble

strr_compress_helper_ML <- function(.data) {
  .data <-
    .data %>%
    group_by(.data$host_ID, .data$listing_type, .data$count, .data$month,
             .data$year) %>%
    summarize(dates = list(.data$date)) %>%
    ungroup()

  single_date <-
    .data %>%
    filter(map(.data$dates, length) == 1) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, ~{.x}),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, ~{.x}),
                              origin = "1970-01-01")) %>%
    select(.data$host_ID, .data$start_date, .data$end_date, .data$listing_type,
           .data$count)

  one_length <-
    .data %>%
    filter(map(.data$dates, length) != 1,
           map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, min),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, max),
                              origin = "1970-01-01")) %>%
    select(.data$host_ID, .data$start_date, .data$end_date, .data$listing_type,
           .data$count)

  if ({.data %>%
      filter(map(.data$dates, length) != 1,
             map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
      nrow} > 0) {

    remainder <-
      .data %>%
      filter(map(.data$dates, length) != 1,
             map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
      mutate(date_range = map(.data$dates, ~{
        tibble(start_date = .x[which(diff(c(0, .x)) > 1)],
               end_date = .x[which(diff(c(.x, 30000)) > 1)])
      })) %>%
      unnest(.data$date_range) %>%
      select(.data$host_ID, .data$start_date, .data$end_date, .data$listing_type,
             .data$count)

  } else remainder <- single_date[0,]

  bind_rows(single_date, one_length, remainder) %>%
    arrange(.data$host_ID, .data$start_date)
}

