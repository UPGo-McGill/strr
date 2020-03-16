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

  chunk_size <- 10000000
  iterations <- 1


  ## Set up on.exit expression for errors

  on.exit({
    # Flush out any stray multicore processes
    future_map(1:future::nbrOfWorkers(), ~.x)

    # Restore future global export limit
    .Options$future.globals.maxSize <- NULL

    # Print \n so error messages don't collide with progress messages
    if (!quiet) message()
  })


  ## Prepare data.table and future variables

  .datatable.aware = TRUE

  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL

  options(future.globals.maxSize = +Inf)


  ## Check if table is daily or host

  if (inherits(data, "strr_daily") | names(data)[1] == "property_ID") {

    helper_progress_message("Daily table identified.")

    daily <- TRUE

  } else if (inherits(data, "strr_host") | names(data)[1] == "host_ID") {

    helper_progress_message("Host table identified.")

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_host`.")


  ### SET BATCH PROCESSING STRATEGY ############################################

  if (nrow(data) > chunk_size) {

    iterations <- ceiling(nrow(data) / chunk_size)

    helper_progress_message(
      "Table is larger than 10,000,000 rows. It will be processed in ",
      iterations,
      " batches.")

  }


  ### STORE EXTRA FIELDS AND TRIM DATA #########################################

  setDT(data)

  if (daily) {

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



  ### PROCESS FOR SMALL TABLE ##################################################

  # Just run strr_expand_helper directly
  if (iterations == 1) {

    helper_progress_message("(1/2) Beginning expansion, using {helper_plan()}.",
                            .type = "progress")

    data <- strr_expand_helper(data, daily, quiet)

  } else {


  ### PROCESS FOR LARGE TABLE ##################################################

    # Split data into 10-million-line chunks
    data_list <- list()
    length(data_list) <- iterations

    # Process each batch sequentially
    for (i in seq_len(iterations)) {

      helper_progress_message("(", i, "/", iterations + 1, ") Expanding batch ",
        i, ", using {helper_plan()}.", .type = "progress")

      data_list[[i]] <-
        data %>%
        slice(((i - 1) * chunk_size + 1):(i * chunk_size)) %>%
        strr_expand_helper(daily, quiet)

    }

    # Bind batches together
    data <- rbindlist(data_list)

  }


  ### REJOIN TO ADDITIONAL FIELDS, THEN ARRANGE COLUMNS ########################

  helper_progress_message(
    "(", iterations + 1, "/", iterations + 1,
    ") Arranging table and joining additional fields.", .type = "open")

  if (daily) {

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
      setDT(data)[order(host_ID, date)] %>%
      select(.data$host_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date)

  }

  helper_progress_message(
    "(", iterations + 1, "/", iterations + 1,
    ") Table arranged and additional fields joined.", .type = "close")


  ### SET CLASS OF OUTPUT ######################################################

  if (daily) {
    class(data) <- append(class(data), "strr_daily")
  } else {
    class(data) <- append(class(data), "strr_host")
  }


  ### OUTPUT DATA FRAME ########################################################

  helper_progress_message("Expansion complete.", .type = "final")

  on.exit(.Options$future.globals.maxSize <- NULL)

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
  setDT(data)


  ### SPLIT DATA ###############################################################

  # Split by property_ID for daily and host_ID for host
  if (daily_flag) {
    data[, col_split := substr(property_ID, 1, 6)]
  } else {
    data[, col_split := substr(host_ID, 1, 3)]
  }

  data_list <-
    split(data, by = "col_split", keep.by = FALSE) %>%
    helper_table_split()


  ### EXPAND TABLE #############################################################

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

  # Flush out any stray multicore processes
  future_map(1:future::nbrOfWorkers(), ~.x)

  return(data)

}
