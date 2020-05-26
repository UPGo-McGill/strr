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
#' @importFrom rlang .data
#' @export

strr_expand <- function(data, quiet = FALSE) {

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }

  # Check if table is daily or host
  if (inherits(data, "strr_daily") | names(data)[1] == "property_ID") {
    daily <- TRUE
  } else if (inherits(data, "strr_host") | names(data)[1] == "host_ID") {
    daily <- FALSE
  } else stop("Input table must be of class `strr_daily` or `strr_host`.")


  ## Check for maximum size ----------------------------------------------------

  if (nrow(data) > 3e8) {
    if (sum(as.numeric(data$end_date) - as.numeric(data$start_date) + 1) >
        2294398000) {
      stop("Output table will exceed maximum data frame size (2 billion rows).")
    }
  }


  ## Prepare data.table and future variables -----------------------------------

  .datatable.aware = TRUE

  # Silence R CMD check for data.table fields
  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL

  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    # Remove limit on globals size
    options(future.globals.maxSize = +Inf)

    # Set up on.exit expression for errors
    on.exit({
      # Flush out any stray multicore processes
      furrr::future_map(1:future::nbrOfWorkers(), ~.x)

      # Restore future global export limit
      .Options$future.globals.maxSize <- NULL

    })
  }


  ## Prepare batches and progress reporting ------------------------------------

  # Enable progress bars if quiet == FALSE
  progress <- !quiet

  # Disable progress bars if {progressr} is not installed
  if (!requireNamespace("progressr", quietly = TRUE)) {
    progress <- FALSE
    .strr_env$pb <- function() NULL
  }

  chunk_size <- 1e7
  iterations <- 1


  ### SET BATCH PROCESSING STRATEGY ############################################

  if (nrow(data) > chunk_size) {

    iterations <- ceiling(nrow(data) / chunk_size)

    if (daily) {
      helper_message("Daily table identified. It will be processed in ",
                     iterations, " batches.")
    } else {
      helper_message("Host table identified. It will be processed in ",
                     iterations, " batches.")
    }
  } else {
    if (daily) {helper_message("Daily table identified.")
    } else helper_message("Host table identified.")
  }


  ### STORE EXTRA FIELDS AND TRIM DATA #########################################

  data.table::setDT(data)

  if (daily) {

    # These fields are per-property
    join_fields <- data[, data.table::last(.SD), by = property_ID
                        ][, c("start_date", "end_date", "status", "booked_date",
                              "price", "res_ID") := NULL]

    # Just keep data necessary for expansion
    data[, c("host_ID", "listing_type", "housing", "country", "region", "city"
             ) := NULL]

  }


  ### PROCESS FOR SMALL TABLE ##################################################

  if (iterations == 1) {

    helper_message("(1/2) Expanding table, using {helper_plan()}.")

    if (progress) {

      handler_strr("Expanding row")

      progressr::with_progress({

        # Initialize progress bar
        .strr_env$pb <- progressr::progressor(steps = nrow(data))

        data <- helper_expand(data, daily)

      })

    } else data <- helper_expand(data, daily)


  ### PROCESS FOR LARGE TABLE ##################################################

  } else {

    # Split data into 10-million-line chunks
    data_list <- vector("list", iterations)

    # Process each batch sequentially
    for (i in seq_len(iterations)) {

      helper_message("(", i, "/", iterations + 1, ") Expanding batch ", i,
                     ", using {helper_plan()}.")

      range_1 <- (i - 1) * chunk_size + 1
      range_2 <- min(i * chunk_size, nrow(data))

      if (progress) {

        handler_strr("Expanding row")

        progressr::with_progress({

          # Initialize progress bar
          .strr_env$pb <-
            progressr::progressor(steps = nrow(data[range_1:range_2]))

          data_list[[i]] <- helper_expand(data[range_1:range_2], daily)

        })

      } else data_list[[i]] <- helper_expand(data[range_1:range_2], daily)

    }

    # Bind batches together
    data <- data.table::rbindlist(data_list)

  }


  ### REJOIN TO ADDITIONAL FIELDS, THEN ARRANGE COLUMNS ########################

  helper_message("(", iterations + 1, "/", iterations + 1, ") Arranging table.",
                 .type = "open")

  if (daily) {

    data <- data.table::setDT(data)[order(property_ID, date)]

    data <- dplyr::left_join(data, join_fields, by = "property_ID")

    data <- dplyr::as_tibble(data)

    data <- dplyr::select(
      data, .data$property_ID, .data$date, dplyr::everything())

  } else {

    data <- data.table::setDT(data)[order(host_ID, date)]

    data <- dplyr::as_tibble(data)

    data <- dplyr::select(data, .data$host_ID, .data$date, dplyr::everything())

  }

  helper_message("(", iterations + 1, "/", iterations + 1, ") Table arranged.",
                 .type = "close")


  ### SET CLASS OF OUTPUT ######################################################

  if (daily) {
    class(data) <- append(class(data), "strr_daily")
  } else {
    class(data) <- append(class(data), "strr_host")
  }


  ### OUTPUT DATA FRAME ########################################################

  helper_message("Expansion complete.", .type = "final")

  if (requireNamespace("future", quietly = TRUE)) {
    on.exit(.Options$future.globals.maxSize <- NULL)
  }

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
#' @return A table with one row per date and all other fields returned
#' unaltered.
#' @importFrom dplyr %>%

helper_expand <- function(data, daily_flag) {

  ### INITIALIZE OBJECTS #######################################################

  .datatable.aware = TRUE

  property_ID <- start_date <- end_date <- col_split <- host_ID <- NULL

  data.table::setDT(data)


  ## Define map_* --------------------------------------------------------------

  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    map_dfr <- furrr::future_map_dfr

  } else map_dfr <- purrr::map_dfr


  ### DEFINE FUNCTION TO BE MAPPED #############################################

  expand_fun <- function(.x) {

    # Iterate progress bar
    if (requireNamespace("progressr", quietly = TRUE)) {
      .strr_env$pb(amount = nrow(.x))
    }

    data.table::setDT(.x)

    # Add new date field
    .x[, date := list(list(start_date:end_date)), by = 1:nrow(.x)]

    # Unnest
    .x <-
      .x[, lapply(.SD, unlist), by = 1:nrow(.x)
         ][, c("nrow", "start_date", "end_date") := NULL]

    .x[, date := as.Date(date, origin = "1970-01-01")]
  }



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
  threads <- data.table::setDTthreads(1)

  # Run function
  data <- map_dfr(data_list, expand_fun)

  # Restore DT threads
  data.table::setDTthreads(threads)

  return(data)

}
