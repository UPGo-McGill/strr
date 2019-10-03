#' Function to expand compressed STR tables
#'
#' \code{strr_expand} takes an STR file compressed in the UPGo DB format and
#' expands it to a one-row-per-date format.
#'
#' A function for expanding compressed daily activity tables from AirDNA or the
#' ML summary tables UPGo produces. The function will also optionally truncate
#' the table by a supplied date range.
#'
#' @param .data A table in compressed UPGo DB format (e.g. created by running
#' \code{\link{strr_compress}}). Currently daily activity files and ML daily
#' summary tables are recognized.
#' @param start A character string of format YYYY-MM-DD indicating the
#'   first date to be provided in the output table. If NULL (default), the
#'   earliest date present in the data will be used.
#' @param end A character string of format YYYY-MM-DD indicating the
#'   last date to be provided in the output table. If NULL (default), the
#'   latest date present in the data will be used.
#' @param cores A positive integer scalar. How many processing cores should be
#'   used to perform the computationally intensive numeric integration step?
#' @param n_chunks A positive integer scalar. How many elements should the
#' table be split into for multicore processing? If cores == 1, this argument
#' is ignored.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A table with one row per date and all other fields returned
#' unaltered.
#' @importFrom dplyr %>% bind_rows filter mutate select
#' @importFrom purrr map2
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @export

strr_expand <- function(.data, start = NULL, end = NULL, cores = 1,
                        n_chunks = 10000, quiet = FALSE) {

  time_1 <- Sys.time()

  ## ERROR CHECKING AND ARGUMENT INITIALIZATION

  # Check that cores is an integer > 0

  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }

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


  ## PREPARE DATE FIELD

  if (!quiet) {message("Preparing new date field. (",
                       substr(Sys.time(), 12, 19), ")")}

  .data <-
    .data %>%
    mutate(date = map2(.data$start_date, .data$end_date, ~{.x:.y}))


  ## SINGLE-CORE VERSION

  if (cores == 1) {

    if (!quiet) {message("Beginning expansion. (",
                         substr(Sys.time(), 12, 19), ")")}

    .data <-
      .data %>%
      unnest(cols = c(date)) %>%
      mutate(date = as.Date(.data$date, origin = "1970-01-01"))

  ## MULTI-CORE VERSION

  } else {

    if (!quiet) {message("Splitting table for multi-core processing. (",
                         substr(Sys.time(), 12, 19), ")")}

    daily_list <-
      split(.data, ceiling(1:nrow(.data)/n_chunks))

    if (!quiet) {message("Beginning expansion. (",
                         substr(Sys.time(), 12, 19), ")")}

        .data <-
      pbapply::pblapply(daily_list, function(x) {
        x %>%
          unnest(cols = c(date)) %>%
          mutate(date = as.Date(.data$date, origin = "1970-01-01"))
      }, cl = cores) %>%
      bind_rows()
  }

  ## ARRANGE COLUMNS

  if (length(.data) == 16) {
    .data <-
      .data %>%
      select(.data$property_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date)
  } else {
    .data <-
      .data %>%
      select(.data$host_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date)
  }

  ## OPTIONALLY TRIM BASED ON START/END DATE

  if (!missing(start)) {
    .data <- filter(.data, .data$date >= start)
  }

  if (!missing(end)) {
    .data <- filter(.data, .data$date <= end)
  }


  ## OUTPUT DATA FRAME

  total_time <- Sys.time() - time_1

  if (!quiet) {message("Expansion complete. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (!quiet) {message("Total time: ",
                      substr(total_time, 1, 5), " ",
                      attr(total_time, "units"), ".")}

  return(.data)
}
