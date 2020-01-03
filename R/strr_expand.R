#' Function to expand compressed STR tables
#'
#' \code{strr_expand} takes an STR file compressed in the UPGo DB format and
#' expands it to a one-row-per-date format.
#'
#' A function for expanding compressed daily activity tables from AirDNA or the
#' ML summary tables UPGo produces, optionally truncating the table by a
#' supplied date range. The function can take advantage of multiprocess and/or
#' remote computation options if a plan is set with the \code{future} package.
#'
#' @param data A table in compressed UPGo DB format (e.g. created by running
#' \code{\link{strr_compress}}). Currently daily activity files and ML daily
#' summary tables are recognized.
#' @param start A character string of format YYYY-MM-DD indicating the
#'   first date to be provided in the output table. If NULL (default), the
#'   earliest date present in the data will be used.
#' @param end A character string of format YYYY-MM-DD indicating the
#'   last date to be provided in the output table. If NULL (default), the
#'   latest date present in the data will be used.
#' @param chunk_size A positive integer scalar. How large should each element be
#' when the table is split for multicore processing? Larger elements should lead
#' to slightly faster processing times but higher memory usage, so low values
#' are recommended for RAM-constrained computers.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A table with one row per date and all other fields returned
#' unaltered.
#' @importFrom dplyr %>% bind_rows filter mutate select
#' @importFrom furrr future_map_dfr
#' @importFrom purrr map2
#' @importFrom rlang .data
#' @importFrom stringr str_detect
#' @importFrom tidyr unnest
#' @export

strr_expand <- function(data, start = NULL, end = NULL, chunk_size = 1000,
                        quiet = FALSE) {

  time_1 <- Sys.time()

  ## ERROR CHECKING AND ARGUMENT INITIALIZATION

  # Remove future global export limit

  options(future.globals.maxSize = +Inf)
  on.exit(.Options$future.globals.maxSize <- NULL)

  # Check that dates are coercible to date class, then coerce them

  if (!missing(start)) {
    start <- tryCatch(as.Date(start), error = function(e) {
      stop(paste0('The value of `start`` ("', start,
                  '") is not coercible to a date.'))
    })}

  if (!missing(end)) {
    end <- tryCatch(as.Date(end), error = function(e) {
      stop(paste0('The value of `end` ("', end,
                  '") is not coercible to a date.'))
    })}

  # Remove strr class as workaround to unnest failing

  class(data) <- class(data)[!str_detect(class(data), "strr")]


  ## STORE EXTRA FIELDS AND TRIM .DATA

  if (length(data) == 13) {
    join_fields <-
      data %>%
      group_by(.data$property_ID) %>%
      filter(.data$start_date == max(.data$start_date)) %>%
      ungroup() %>%
      select(.data$property_ID, .data$host_ID, .data$listing_type,
             .data$housing, .data$country, .data$region, .data$city)

    data <-
      data %>%
      select(.data$property_ID, .data$start_date, .data$end_date, .data$status,
             .data$booked_date, .data$price, .data$res_ID)
    }


  ## PREPARE DATE FIELD

  if (!quiet) {message("Preparing new date field. (",
                       substr(Sys.time(), 12, 19), ")")}

  data <-
    data %>%
    mutate(date = map2(.data$start_date, .data$end_date, ~{.x:.y}))

  suppressWarnings(
    daily_list <-
      split(data, 1:min(chunk_size, nrow(data)))
    )


  ## EXPAND TABLE

  if (!quiet) {message("Beginning expansion, using ", helper_plan(), ". (",
                       substr(Sys.time(), 12, 19), ")")}

  data <-
    daily_list %>%
    future_map_dfr(~{
      .x %>%
        unnest(cols = c(date)) %>%
        mutate(date = as.Date(.data$date, origin = "1970-01-01"))
      },
      # Suppress progress bar if quiet == TRUE or the plan is remote
      .progress = helper_progress(quiet)
      )

  ## REJOIN TO ADDITIONAL FIELDS, THEN ARRANGE COLUMNS

  if (!quiet) {message("Joining additional fields to table. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (length(data) == 8) {
    data <-
      data %>%
      left_join(join_fields, by = "property_ID") %>%
      select(.data$property_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date)
  } else {
    data <-
      data %>%
      select(.data$host_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date)
  }

  ## OPTIONALLY TRIM BASED ON START/END DATE

  if (!missing(start)) {
    data <- filter(data, .data$date >= start)
  }

  if (!missing(end)) {
    data <- filter(data, .data$date <= end)
  }


  ## SET CLASS OF OUTPUT

  if (names(data)[1] == "property_ID") {
    class(data) <- c(class(data), "strr_daily")
  } else {
    class(data) <- c(class(data), "strr_multi")
  }


  ## OUTPUT DATA FRAME

  total_time <- Sys.time() - time_1

  if (!quiet) {message("Expansion complete. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (!quiet) {message("Total time: ",
                      substr(total_time, 1, 5), " ",
                      attr(total_time, "units"), ".")}

  return(data)
}
