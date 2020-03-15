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
#' \code{\link{strr_compress}}). Currently daily activity tables and host tables
#' are recognized.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A table with one row per date and all other fields returned
#' unaltered.
#' @importFrom data.table last setDT setDTthreads setnames
#' @importFrom dplyr %>% as_tibble everything filter left_join mutate select
#' @importFrom dplyr slice
#' @importFrom furrr future_map_dfr
#' @importFrom rlang .data
#' @export

strr_expand <- function(data, quiet = FALSE) {

  time_1 <- Sys.time()

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  ## Prepare data.table variables

  .datatable.aware = TRUE

  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL


  ## Remove future global export limit

  options(future.globals.maxSize = +Inf)

  on.exit(.Options$future.globals.maxSize <- NULL)


  ## Check if table is daily or host

  if (inherits(data, "strr_daily") | names(data)[1] == "property_ID") {

    helper_progress_message("Daily table identified.")

    daily <- TRUE

  } else if (inherits(data, "strr_host") | names(data)[1] == "host_ID") {

    helper_progress_message("Host table identified.")

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_host`.")


  ### PROCESS FOR SMALL TABLE ##################################################

  # Just run strr_expand_helper directly
  if (nrow(data) < 50000000) data <- strr_expand_helper(data, daily, quiet)


  ### PROCESS FOR LARGE TABLE ##################################################

  # Split data into 50-million-line chunks
  if (nrow(data) >= 50000000) {

    iterations <- ceiling(nrow(data) / 50000000)

    helper_progress_message(
      "Table is larger than 50,000,000 rows. It will be processed in ",
      iterations,
      " batches.")

    data_list <- list()
    length(data_list) <- iterations

    for (i in seq_len(iterations)) {

      helper_progress_message("Processing batch ", i, ".")

      data_list[[i]] <-
        data %>%
        slice(((i - 1) * 50000000 + 1):(i * 50000000)) %>%
        strr_expand_helper(daily, quiet)

    }

    data <- bind_rows(data_list)

  }


  ### SET CLASS OF OUTPUT ######################################################

  if (daily) {
    class(data) <- append(class(data), "strr_daily")
  } else {
    class(data) <- append(class(data), "strr_host")
  }


  ### OUTPUT DATA FRAME ########################################################

  helper_progress_message("Expansion complete.", .type = "final")

  return(data)
}



#' Helper function to expand compressed STR tables
#'
#' \code{strr_expand_helper} performs the daily/host table expansion within
#' \code{\link{strr_expand}}.
#'#'
#' @param data A table in compressed UPGo DB format (e.g. created by running
#' \code{\link{strr_compress}}). Currently daily activity table and host tables
#' are recognized.
#' @param daily_flag A logical scalar. Is the input table a daily table (TRUE)
#' or a host table (FALSE)?
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A table with one row per date and all other fields returned
#' unaltered.

strr_expand_helper <- function(data, daily_flag, quiet) {

  .datatable.aware = TRUE

  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL


  ### STORE EXTRA FIELDS AND TRIM DATA #########################################

  setDT(data)

  if (daily_flag) {

    # These fields are per-property
    join_fields <- data[, last(.SD), by = property_ID
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


  ### SPLIT DATA ###############################################################


  ## Split by property_ID for daily and host_ID for host

  helper_progress_message(
    "(1/3) Splitting data for processing.", .type = "open")

  if (daily_flag) {
    data[, col_split := substr(property_ID, 1, 6)]
  } else {
    data[, col_split := substr(host_ID, 1, 3)]
  }

  data_list <-
    split(data, by = "col_split", keep.by = FALSE) %>%
    helper_table_split()

  helper_progress_message("(1/3) Data split for processing.", .type = "close")


  ### EXPAND TABLE #############################################################

  helper_progress_message("(2/3) Beginning expansion, using {helper_plan()}.",
                          .type = "progress")

  # Make sure data.table is single-threaded within the helper
  threads <- setDTthreads(1)

  data <-
    data_list %>%
    future_map_dfr(~{

      setDT(.x)

      # Add new date field
      .x[, date := list(list(start_date:end_date)), by = 1:nrow(.x)]

      # Unnest
      .x[, lapply(.SD, unlist), by = 1:nrow(.x)][, nrow := NULL] %>%
        as_tibble() %>%
        mutate(date = as.Date(.data$date, origin = "1970-01-01"))
    },
    # Suppress progress bar if quiet == TRUE or the plan is remote
    .progress = helper_progress()
    )

  # Restore DT threads
  setDTthreads(threads)


  ### REJOIN TO ADDITIONAL FIELDS, THEN ARRANGE COLUMNS ########################

  helper_progress_message(
    "(3/3) Joining additional fields to table.", .type = "open")

  if (daily_flag) {

    data <-
      setDT(data)[order(property_ID, date)] %>%
      # Join fields which need to be duplicated for specific date ranges
      left_join(add_fields, by = c("property_ID", "date")) %>%
      tidyr::fill(.data$status:.data$res_ID) %>%
      # Join fields which need to be duplicated for specific properties
      left_join(join_fields, by = "property_ID") %>%
      select(.data$property_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date) %>%
      as_tibble()

  } else {

    data <-
      data %>%
      select(.data$host_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date) %>%
      arrange(.data$host_ID, .data$date)

  }

  helper_progress_message(
    "(3/3) Additional fields joined to table.", .type = "close")

  return(data)

}
