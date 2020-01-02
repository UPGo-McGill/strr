#' Function to compress daily STR tables into UPGo DB format
#'
#' \code{strr_compress} takes daily tables (either monthly daily tables from
#' AirDNA or ML summary tables produced by UPGo) and compresses them into the
#' UPGo database storage format.
#'
#' A function for compressing daily activity tables. It takes either AirDNA
#' daily tables or UPGo multilisting summary tables which have been processed
#' using \code{\link{strr_process_daily}} or \code{\link{strr_process_multi}},
#' and converts them into a more storage-efficient one-activity-block-per-row
#' format.
#'
#' The output can subsequently be restored to a non-compressed format using
#' \code{\link{strr_expand}}.
#'
#' @param data A daily table in either the processed UPGo daily format or the
#' processed UPGo multilisting format.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A compressed daily table, ready for upload to a remote database.
#' @importFrom data.table setDT
#' @importFrom dplyr %>% arrange bind_rows filter group_by group_split mutate
#' @importFrom dplyr pull select
#' @importFrom furrr future_map_dfr
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_compress <- function(data, quiet = FALSE) {

  time_1 <- Sys.time()

  ## Error checking and initialization

  .datatable.aware = TRUE

  # Remove future global export limit

  options(future.globals.maxSize = +Inf)
  on.exit(.Options$future.globals.maxSize <- NULL)

  # Check if table is daily or ML

  if ("strr_daily" %in% class(data) | names(data)[1] == "property_ID") {

    if (!quiet) {message("Daily table identified. (",
                         substr(Sys.time(), 12, 19), ")")}

    daily <- TRUE

  } else if ("strr_multi" %in% class(data) | names(data)[1] == "host_ID") {

    if (!quiet) {message("Multilisting table identified. (",
                         substr(Sys.time(), 12, 19), ")")}

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_multi`.")


  ## Store invariant fields for later

  if (daily) {

    join_fields <-
      data %>%
      group_by(.data$property_ID) %>%
      filter(.data$date == max(.data$date)) %>%
      ungroup() %>%
      select(.data$property_ID, .data$host_ID, .data$listing_type, .data$housing,
             .data$country, .data$region, .data$city)

    data <-
      data %>%
      select(-.data$host_ID, -.data$listing_type, -.data$housing, -.data$country,
             -.data$region, -.data$city)
  }


  ## Produce month and year columns if data spans multiple months

  if (lubridate::month(min(data$date)) != lubridate::month(max(data$date))) {

    if (!quiet) {message("Splitting table by year and month. (",
                         substr(Sys.time(), 12, 19), ")")}

    data <-
      data %>%
      mutate(month = lubridate::month(data$date),
             year = lubridate::year(data$date))

  }


   ## Compress processed data file

  data_list <-
    data %>%
    group_split(.data$property_ID)

    if (length(data_list) > 100) {

      data_list <- purrr::map(1:100, function(i) {
        bind_rows(
          data_list[(floor(as.numeric(length(data_list)) * (i - 1) / 100) +
                        1):floor(as.numeric(length(data_list)) * i / 100)])
      })}

  if (!quiet) {message("Beginning compression, using ", helper_plan(), ". (",
                       substr(Sys.time(), 12, 19), ")")}

  if (daily) {

    compressed <-
      data_list %>%
      future_map_dfr(strr_compress_helper,
                     # Suppress progress bar if !quiet or the plan is remote
                     .progress = helper_progress(quiet)) %>%
      left_join(join_fields, by = "property_ID")

  } else {

    compressed <-
      data_list %>%
      future_map_dfr(strr_compress_helper_ML,
                     # Suppress progress bar if !quiet or the plan is remote
                     .progress = helper_progress(quiet))

  }


  ## Arrange output and set class

  if (!quiet) {message("Arranging output table. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (daily) {
    compressed <- arrange(compressed, .data$property_ID, .data$start_date)
    class(compressed) <- c(class(compressed), "strr_daily")
    } else {
      compressed <- arrange(compressed, .data$host_ID, .data$start_date)
      class(compressed) <- c(class(compressed), "strr_multi")
      }


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
#' @param data The processed daily table generated through the strr_compress
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
#' @param data The processed ML table generated through the strr_compress
#' function.
#' @return The output will be a compressed ML table.
#' @importFrom dplyr %>% arrange bind_rows filter group_by group_by_at mutate
#' @importFrom dplyr  select summarize ungroup vars
#' @importFrom purrr map map_dbl
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @importFrom tibble tibble

strr_compress_helper_ML <- function(data) {

  # Group .data by all columns except date
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
    select(.data$host_ID, .data$start_date, .data$end_date, .data$listing_type,
           .data$housing, .data$active, .data$count)

  one_length <-
    data %>%
    filter(map(.data$dates, length) != 1,
           map(.data$dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
    mutate(start_date = as.Date(map_dbl(.data$dates, min),
                                origin = "1970-01-01"),
           end_date = as.Date(map_dbl(.data$dates, max),
                              origin = "1970-01-01")) %>%
    select(.data$host_ID, .data$start_date, .data$end_date, .data$listing_type,
           .data$housing, .data$active, .data$count)

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
      select(.data$host_ID, .data$start_date, .data$end_date, .data$listing_type,
             .data$housing, .data$active, .data$count)

  } else remainder <- single_date[0,]

  bind_rows(single_date, one_length, remainder) %>%
    arrange(.data$host_ID, .data$start_date)
}

