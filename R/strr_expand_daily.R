#' Function to expand compressed daily STR tables
#'
#' \code{strr_expand_daily} takes STR daily file compressed in the UPGo DB
#' format and expands it to a one-row-per-date format.
#'
#' A function for expanding compressed daily activity tables from AirDNA. The
#' function will also optionally truncated the table by a supplied date range.
#'
#' @param daily A daily table in the compressed UPGo DB format (e.g. created
#' by running \code{\link{strr_compress_daily}} on a raw daily table from
#' AirDNA).
#' @param start A character string of format YYYY-MM-DD indicating the
#'   first date to be provided in the output table. If NULL (default), the
#'   earliest date present in the data will be used.
#' @param end A character string of format YYYY-MM-DD indicating the
#'   last date to be provided in the output table. If NULL (default), the
#'   latest date present in the data will be used.
#' @param cores A positive integer scalar. How many processing cores should be
#'   used to perform the computationally intensive numeric integration step?
#' @return A table of daily STR activity with one row per date and all other
#' fields returned unaltered.
#' @importFrom dplyr %>% bind_rows filter mutate select
#' @importFrom purrr map2
#' @importFrom rlang .data
#' @importFrom tidyr unnest
#' @export

strr_expand_daily <- function(daily, start = NULL, end = NULL, cores = 1) {

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

  daily <-
    daily %>%
    mutate(date = map2(.data$start_date, .data$end_date, ~{.x:.y}))


  ## SINGLE-CORE VERSION

  if (cores == 1) {

    daily <-
      daily %>%
      unnest(cols = c(date)) %>%
      mutate(date = as.Date(.data$date, origin = "1970-01-01")) %>%
      select(.data$property_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date)

  ## MULTI-CORE VERSION

  } else {

    daily_list <-
      split(daily, ceiling(1:nrow(daily)/1000))

    daily <-
      pbapply::pblapply(daily_list, function(x) {
        x %>%
          unnest(cols = c(date)) %>%
          mutate(date = as.Date(.data$date, origin = "1970-01-01")) %>%
          select(.data$property_ID, .data$date, everything(), -.data$start_date,
                 -.data$end_date)
      }, cl = cores) %>%
      bind_rows()
  }


  ## OPTIONALLY TRIM BASED ON START/END DATE

  if (!missing(start)) {
    daily <- filter(daily, .data$date >= start)
  }

  if (!missing(end)) {
    daily <- filter(daily, .data$date <= end)
  }


  ## OUTPUT DATA FRAME

  return(daily)
}
