#' Function to compress raw daily STR tables into UPGo DB format
#'
#' \code{strr_compress_daily} takes raw monthly daily tables from AirDNA and
#' compresses them into the UPGo database storage format.
#'
#' A function for compressing the daily activity tables supplied each month from
#' AirDNA into a more storage-efficient one-activity-block-per-row format. The
#' function also produces error files which identify possible corrupt or missing
#' lines in the input file.
#'
#' @param daily A daily table in the raw AirDNA format.
#' @param output_date A character string of the format "YYYY-MM" identifying the
#' year and month of the daily table. If a value is supplied to this argument,
#' the results of the function will be written to disk in a subfolder
#' "/output/YYYY/". If the argument is left NULL (its default), nothing will be
#' written to disk.
#' @param cores A positive integer scalar. How many processing cores should be
#' used to perform the computationally intensive compression step?
#' @param quiet A logical vector. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be a list with three elements: 1) the compressed
#' daily table, ready for upload to a remote database; 2) an error table
#' identifying corrupt or otherwise invalid row entries; 3) a missing_rows
#' table identifying property_IDs with missing dates in between their first and
#' last date entries, and therefore potentially missing data.
#' @importFrom data.table setDT
#' @importFrom dplyr %>% bind_rows filter mutate pull select
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_compress_daily <- function(daily, output_date = NULL, cores = 1,
                                quiet = FALSE) {

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
    select(.data$`Property ID`, .data$Date, .data$Status, .data$`Booked Date`,
           .data$`Price (USD)`, .data$`Reservation ID`) %>%
    rlang::set_names(c("property_ID", "date", "status", "booked_date", "price",
                "res_ID"))

  ## Find rows with readr errors and add to error file

  if (!quiet) {message("Beginning error check.")}

  error <-
    readr::problems(daily) %>%
    filter(.data$expected != "10 columns",
           .data$expected != "no trailing characters") %>%
    pull(row) %>%
    daily[.,]

  error_vector <-
    readr::problems(daily) %>%
    filter(.data$expected != "10 columns",
           .data$expected != "no trailing characters") %>%
    pull(row)

  if (length(error_vector) > 0) {daily <- daily[-error_vector,]}

  if (!quiet) {message("Initial import errors identified.")}

  ## Find rows with missing property_ID, date or status

  error <-
    daily %>%
    filter(is.na(.data$property_ID) | is.na(.data$date) |
             is.na(.data$status)) %>%
    rbind(error)

  daily <-
    daily %>%
    filter(!is.na(.data$property_ID), !is.na(.data$date), !is.na(.data$status))

  if (!quiet) {
    message("Rows with missing property_ID, date or status identified.")}

  ## Check status

  error <-
    daily %>%
    filter(!(.data$status %in% c("A", "U", "B", "R"))) %>%
    rbind(error)

  daily <-
    daily %>%
    filter(.data$status %in% c("A", "U", "B", "R"))

  if (!quiet) {message("Rows with invalid status identified.")}

  ## Remove duplicate listing entries by price, but don't add to error file

  daily <-
    daily %>%
    filter(!is.na(.data$price))

  if (!quiet) {message("Duplicate rows removed.")}

  ## Find missing rows

  .N = property_ID = NULL # due to NSE notes in R CMD check

  setDT(daily)
  missing_rows <-
    daily[, .(count = .N,
                 full_count = as.integer(max(date) - min(date) + 1)),
          by = property_ID]

  missing_rows <-
    missing_rows %>%
    as_tibble() %>%
    mutate(dif = .data$full_count - .data$count) %>%
    filter(.data$dif != 0)

  if (!quiet) {message("Missing rows identified.")}

  ## Produce month and year columns to make unique entries

  daily <-
    daily %>%
    as_tibble() %>%
    mutate(month = lubridate::month(.data$date),
           year = lubridate::year(.data$date))

  ## Compress processed daily file

  if (!quiet) {message("Error check complete. Beginning compression.")}

  if (cores > 1) {

    daily_list <- split(daily, daily$property_ID)

    daily_list <- purrr::map(1:100, function(i) {
      bind_rows(
        daily_list[(floor(length(daily_list) * (i - 1) / 100) +
                      1):floor(length(daily_list) * i / 100)])
    })

    daily2 <-
      daily_list %>%
      pbapply::pblapply(strr_compress_helper, cl = 3) %>%
      do.call(rbind, .)

  } else daily <- strr_compress_helper(daily)


  ## Write files to disk if output_date is specified

  if (!missing(output_date)) {

    if (!quiet) {message("Compression complete. Writing output to disk.")}

    save(daily, file = paste0("output/", substr(output_date, 1, 4), "/daily_",
                              output_date, ".Rdata"))
    readr::write_csv(error, paste0("output/", substr(output_date, 1, 4),
                                   "/error_", output_date, ".csv"))
    readr::write_csv(missing_rows, paste0("output/", substr(output_date, 1, 4),
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
#' @importFrom purrr map map_dbl
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @importFrom tibble tibble

strr_compress_helper <- function(daily) {
  daily <-
    daily %>%
    group_by(.data$property_ID, .data$status, .data$booked_date, .data$price,
             .data$res_ID, .data$month, .data$year) %>%
    summarize(dates = list(.data$date)) %>%
    ungroup()

  single_date <-
    daily %>%
    filter(map(.data$dates, length) == 1) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, ~{.x}),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, ~{.x}),
                              origin = "1970-01-01")) %>%
    select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
           .data$booked_date, .data$price, .data$res_ID)

  one_length <-
    daily %>%
    filter(map(.data$dates, length) != 1,
           map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, min),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, max),
                              origin = "1970-01-01")) %>%
    select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
           .data$booked_date, .data$price, .data$res_ID)

  if ({daily %>%
      filter(map(.data$dates, length) != 1,
             map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
      nrow} > 0) {

    remainder <-
      daily %>%
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
