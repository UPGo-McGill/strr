#' Expand compressed STR tables
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
#' @param chunk_size TKTK
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A table with one row per date and all other fields returned
#' unaltered.
#' @importFrom rlang .data
#' @export

strr_expand <- function(data, chunk_size = 1e6, quiet = FALSE) {

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  daily_flag <- helper_check_data()
  helper_check_quiet()


  ## Check for maximum size ----------------------------------------------------

  if (nrow(data) > 3e8) {
    if (sum(as.numeric(data$end_date) - as.numeric(data$start_date) + 1) >
        2294398000) {
      stop("Output table will exceed maximum data frame size (2 billion rows).")
    }
  }


  ## Silence R CMD check for data.table fields ---------------------------------

  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL


  ## Prepare batches -----------------------------------------------------------

  iterations <- 1


  ### SET BATCH PROCESSING STRATEGY ############################################

  if (nrow(data) > chunk_size) {

    iterations <- ceiling(nrow(data) / chunk_size)

    if (daily_flag) {
      helper_message("Daily table identified. It will be processed in ",
                     iterations, " batches.")
    } else {
      helper_message("Host table identified. It will be processed in ",
                     iterations, " batches.")
    }
  } else {
    if (daily_flag) {helper_message("Daily table identified.")
    } else helper_message("Host table identified.")
  }


  ### STORE EXTRA FIELDS AND TRIM DATA #########################################

  data.table::setDT(data)

  if (daily_flag) {

    # These fields are per-property
    join_fields <- data[, data.table::last(.SD), by = property_ID
                        ][, c("start_date", "end_date", "status", "booked_date",
                              "price", "res_ID") := NULL]

    # Just keep data necessary for expansion
    data[, c("host_ID", "listing_type", "housing", "country", "region", "city"
             ) := NULL]

  }


  ### PROCESS FOR SMALL TABLE ##################################################

  # Make sure data.table is single-threaded within the helper
  threads <- data.table::setDTthreads(1)

  if (iterations == 1) {

    helper_message("(1/2) Expanding table, using ", helper_plan(), ".")

    data_list <- helper_prepare_expand(data, daily_flag)

    # Run function
    handler_strr("Expanding row")

    with_progress({
      pb <- progressor(steps = nrow(data))
      data <- par_lapply(data_list, function(x) {
        pb(amount = nrow(x))
        helper_expand(x)})
      })

    data <- data.table::rbindlist(data)


  ### PROCESS FOR LARGE TABLE ##################################################

  } else {

    # Split data into chunks
    chunk_list <- vector("list", iterations)

    helper_message("(1/2) Expanding table, using ", helper_plan(), ".")
    handler_strr("Expanding row")

    # Process each batch sequentially
    with_progress({

      pb <- progressor(steps = nrow(data))

      for (i in seq_len(iterations)) {

        range_1 <- (i - 1) * chunk_size + 1
        range_2 <- min(i * chunk_size, nrow(data))

        data_list <- helper_prepare_expand(data[range_1:range_2], daily_flag)

        chunk_list[[i]] <- par_lapply(data_list, function(x) {
          pb(amount = nrow(x))
          helper_expand(x)})

        chunk_list[[i]] <- data.table::rbindlist(chunk_list[[i]])

      }

      # Bind batches together
      data <- data.table::rbindlist(chunk_list)

      })

    # Restore DT threads
    data.table::setDTthreads(threads)

  }


  ### REJOIN TO ADDITIONAL FIELDS, THEN ARRANGE COLUMNS ########################

  helper_message("(2/2) Arranging table.", .type = "open")

  if (daily_flag) {

    data <- data.table::setDT(data)[order(property_ID, date)]
    data <- dplyr::left_join(data, join_fields, by = "property_ID")
    data <- dplyr::as_tibble(data)
    data <- dplyr::select(data, .data$property_ID, .data$date,
                          dplyr::everything())

  } else {

    data <- data.table::setDT(data)[order(host_ID, date)]
    data <- dplyr::as_tibble(data)
    data <- dplyr::select(data, .data$host_ID, .data$date, dplyr::everything())

  }

  helper_message("(2/2) Table arranged.", .type = "close")


  ### OUTPUT DATA FRAME ########################################################

  helper_message("Expansion complete.", .type = "final")

  return(data)
}


#' Helper function to prepare to expand compressed STR tables
#'
#' @param data,daily_flag Arguments passed from the main function.

helper_prepare_expand <- function(data, daily_flag) {

  ## Initialize objects --------------------------------------------------------

  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL
  data.table::setDT(data)


  ## Split data ----------------------------------------------------------------

  # Split by property_ID for daily and host_ID for host
  if (daily_flag) {
    data[, col_split := substr(property_ID, 1, 8)]
  } else {
    data[, col_split := substr(host_ID, 1, 3)]
  }

  data_list <- split(data, by = "col_split", keep.by = FALSE)
  data_list <- helper_table_split(data_list)

  return(data_list)

}


#' Helper function to expand compressed STR tables
#'
#' @param x Argument passed from the main function.

helper_expand <- function(x) {

  start_date <- end_date <- NULL

  data.table::setDT(x)

  # Add new date field
  x[, date := list(list(start_date:end_date)), by = seq_len(nrow(x))]

  # Unnest
  x <- x[, lapply(.SD, unlist), by = 1:nrow(x)
           ][, c("nrow", "start_date", "end_date") := NULL]

  x[, date := as.Date(date, origin = "1970-01-01")]

  }
