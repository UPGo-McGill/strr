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
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A table with one row per date and all other fields returned
#' unaltered.
#' @importFrom data.table setDT setnames
#' @importFrom dplyr %>% everything filter left_join mutate select
#' @importFrom furrr future_map_dfr
#' @importFrom rlang .data
#' @export

strr_expand <- function(data, start = NULL, end = NULL, quiet = FALSE) {

  time_1 <- Sys.time()

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  ## Prepare data.table variables

  .datatable.aware = TRUE

  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL


  ## Remove future global export limit

  options(future.globals.maxSize = +Inf)

  on.exit(.Options$future.globals.maxSize <- NULL)


  ## Check if table is daily or ML

  if (inherits(data, "strr_daily") | names(data)[1] == "property_ID") {

    helper_progress_message("Daily table identified.")

    daily <- TRUE

  } else if (inherits(data, "strr_host") | names(data)[1] == "host_ID") {

    helper_progress_message("Host table identified.")

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_host`.")


  ## Check that dates are coercible to date class, then coerce them

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


  ### STORE EXTRA FIELDS AND TRIM DATA #########################################

  setDT(data)

  ## TKTK Remove 15 once daily DB update is complete
  if (length(data) %in% c(13, 15)) {

    # These fields are per-property
    join_fields <- data[, .SD[1], by = property_ID
                        ][, c("start_date", "end_date", "status", "booked_date",
                              "price", "res_ID") := NULL]

    # These fields are per-property, per-date
    add_fields <-
      data[, c("property_ID", "start_date", "status", "booked_date", "price",
               "res_ID")]

    setnames(add_fields, "start_date", "date")

    # Just keep data necessary for expansion
    data <- data[, .(property_ID, start_date, end_date)]

  }


  ### PREPARE DATE FIELD AND SPLIT DATA ########################################

  helper_progress_message("Preparing new date field.", .type = "open")

  data[, date := list(list(start_date:end_date)), by = 1:nrow(data)]

  helper_progress_message("New date field prepared.", .type = "close")


  ## Split by property_ID for daily and host_ID for host

  helper_progress_message("Splitting data for processing.", .type = "open")

  if (daily) {
    data[, col_split := substr(property_ID, 1, 6)]
    } else {
      data[, col_split := substr(host_ID, 1, 3)]
    }

  data_list <-
    split(data, by = "col_split", keep.by = FALSE) %>%
    helper_table_split()

  helper_progress_message("Data split for processing.", .type = "close")

  ### EXPAND TABLE #############################################################

  helper_progress_message("Beginning expansion, using {helper_plan()}.")

  data <-
    data_list %>%
    future_map_dfr(~{

      setDT(.x)

      .x[, lapply(.SD, unlist), by = 1:nrow(.x)][, nrow := NULL] %>%
        tibble::as_tibble() %>%
        mutate(date = as.Date(.data$date, origin = "1970-01-01"))
      },
      # Suppress progress bar if quiet == TRUE or the plan is remote
      .progress = helper_progress()
      )


  ### REJOIN TO ADDITIONAL FIELDS, THEN ARRANGE COLUMNS ########################

  helper_progress_message("Joining additional fields to table.", .type = "open")

  if (length(data) == 4) {

    data <-
      setDT(data)[order(property_ID, date)] %>%
      # Join fields which need to be duplicated for specific date ranges
      left_join(add_fields, by = c("property_ID", "date")) %>%
      tidyr::fill(.data$status:.data$res_ID) %>%
      # Join fields which need to be duplicated for specific properties
      left_join(join_fields, by = "property_ID") %>%
      select(.data$property_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date) %>%
      tibble::as_tibble()

  } else {

    data <-
      data %>%
      select(.data$host_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date) %>%
      arrange(.data$host_ID, .data$date)

  }

  helper_progress_message("Additional fields joined to table.", .type = "close")


  ### OPTIONALLY TRIM BASED ON START/END DATE ##################################

  if (!missing(start)) {
    data <- filter(data, .data$date >= start)
  }

  if (!missing(end)) {
    data <- filter(data, .data$date <= end)
  }


  ### SET CLASS OF OUTPUT ######################################################

  if (names(data)[1] == "property_ID") {
    class(data) <- append(class(data), "strr_daily")
  } else {
    class(data) <- append(class(data), "strr_host")
  }


  ### OUTPUT DATA FRAME ########################################################

  helper_progress_message("Expansion complete.", .type = "final")

  return(data)
}
