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
#' @param quiet A logical scalar. Should the function execute quietly, or should
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
#' @importFrom dplyr %>% bind_rows filter group_by group_split mutate pull
#' @importFrom dplyr select
#' @importFrom furrr future_map_dfr
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_compress <- function(data, quiet = FALSE) {

  time_1 <- Sys.time()

  ## Error checking and initialization

  .datatable.aware = TRUE


  ## Perform simple compression if the input is an ML table

  if (names(.data)[1] == "host_ID") {

    if (!quiet) {message("ML table identified. (",
                         substr(Sys.time(), 12, 19), ")")}

    date_flag = FALSE

   if (lubridate::month(min(.data$date)) != lubridate::month(max(.data$date))) {

      if (!quiet) {message("Splitting table by year and month. (",
                           substr(Sys.time(), 12, 19), ")")}

      .data <-
        .data %>%
        mutate(month = lubridate::month(.data$date),
               year = lubridate::year(.data$date))

      date_flag = TRUE
    }


      if (!quiet) {message("Splitting table for multicore processing. (",
                           substr(Sys.time(), 12, 19), ")")}

      daily_list <-
        .data %>%
        group_by(.data$host_ID) %>%
        group_split()

      # if (length(daily_list) > 10000 & chunks == TRUE) {
      #
      #   if (!quiet) {message("Reassembling table pieces for compression. (",
      #                        substr(Sys.time(), 12, 19), ")")}
      #
      #   daily_list <- purrr::map(1:10000, function(i) {
      #     bind_rows(
      #       daily_list[(floor(as.numeric(length(daily_list)) *
      #                           (i - 1) / 10000) +
      #                     1):floor(as.numeric(length(daily_list)) * i / 10000)])
      #   })}

      compressed <-
        daily_list %>%
        pbapply::pblapply(strr_compress_helper_ML) %>%
        bind_rows()


    total_time <- Sys.time() - time_1

    if (!quiet) {message("Compression complete. (",
                         substr(Sys.time(), 12, 19), ")")}

    if (!quiet) {message("Total time: ",
                         substr(total_time, 1, 5), " ",
                         attr(total_time, "units"), ".")}

    return(compressed)
  }


  ## Subset data table and rename fields

  .data <-
    .data %>%
    select(.data$`Property ID`, .data$Date, .data$Status, .data$`Booked Date`,
           .data$`Price (USD)`, .data$`Reservation ID`) %>%
    rlang::set_names(c("property_ID", "date", "status", "booked_date", "price",
                "res_ID"))


  ## Produce month and year columns if data spans multiple months

  date_flag = FALSE

  if (lubridate::month(min(data$date)) != lubridate::month(max(data$date))) {

    if (!quiet) {message("Splitting table by year and month. (",
                         substr(Sys.time(), 12, 19), ")")}

    data <-
      data %>%
      mutate(month = lubridate::month(data$date),
             year = lubridate::year(data$date))

    date_flag = TRUE
  }

  ## Remove invariant fields

  join_fields <-
    data %>%
    select(.data$property_ID, .data$host_ID, .data$listing_type, .data$housing,
           .data$country, .data$region, .data$city)

  data <-
    data %>%
    select(-.data$host_ID, -.data$listing_type, -.data$housing, -.data$country,
           -.data$region, -.data$city)

  ## Compress processed data file

  daily_list <-
    data %>%
    group_split(data$property_ID)

    # if (length(daily_list) > 10000 & chunks == TRUE) {
    #
    #
    #   daily_list <- purrr::map(1:10000, function(i) {
    #     bind_rows(
    #       daily_list[(floor(as.numeric(length(daily_list)) * (i - 1) / 10000) +
    #                     1):floor(as.numeric(length(daily_list)) * i / 10000)])
    #   })}

  if (!quiet) {message("Beginning compression, using ", helper_plan(), ". (",
                       substr(Sys.time(), 12, 19), ")")}

  compressed <-
    daily_list %>%
    future_map_dfr(strr_compress_helper) %>%
    left_join(join_fields, by = "property_ID")


  ## Return output

  total_time <- Sys.time() - time_1

  if (!quiet) {message("Compression complete. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (!quiet) {message("Total time: ",
                       substr(total_time, 1, 5), " ",
                       attr(total_time, "units"), ".")}

  return(compressed)
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
#' @importFrom dplyr %>% arrange bind_rows filter group_by group_by_at mutate
#' @importFrom dplyr select summarize ungroup vars
#' @importFrom purrr map map_dbl
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @importFrom tibble tibble

strr_compress_helper <- function(data) {

  # Group data by all columns except date
  data <-
      data %>%
      group_by_at(vars(-.data$date)) %>%
      summarize(dates = list(.data$date)) %>%
      ungroup()

  single_date <-
    data %>%
    filter(map(.data$dates, length) == 1) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, ~{.x}),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, ~{.x}),
                              origin = "1970-01-01")) %>%
    select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
           .data$booked_date, .data$price, .data$res_ID)

  one_length <-
    data %>%
    filter(map(.data$dates, length) != 1,
           map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, min),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, max),
                              origin = "1970-01-01")) %>%
    select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
           .data$booked_date, .data$price, .data$res_ID)

  if ({data %>%
      filter(map(.data$dates, length) != 1,
             map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
      nrow} > 0) {

    remainder <-
      data %>%
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

  bind_rows(single_date, one_length, remainder)
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
#' @importFrom dplyr %>% arrange bind_rows filter group_by group_by_at mutate
#' @importFrom dplyr  select summarize ungroup vars
#' @importFrom purrr map map_dbl
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @importFrom tibble tibble

strr_compress_helper_ML <- function(.data) {

  # Group .data by all columns except date
  .data <-
    .data %>%
    group_by_at(vars(-.data$date)) %>%
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
           .data$housing, .data$active, .data$count)

  one_length <-
    .data %>%
    filter(map(.data$dates, length) != 1,
           map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, min),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, max),
                              origin = "1970-01-01")) %>%
    select(.data$host_ID, .data$start_date, .data$end_date, .data$listing_type,
           .data$housing, .data$active, .data$count)

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
             .data$housing, .data$active, .data$count)

  } else remainder <- single_date[0,]

  bind_rows(single_date, one_length, remainder) %>%
    arrange(.data$host_ID, .data$start_date)
}

