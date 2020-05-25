#' Function to compress daily STR tables into UPGo DB format
#'
#' \code{strr_compress} takes daily tables (either daily activity tables
#' produced through \code{\link{strr_process_daily}} or daily host tables
#' produced through \code{\link{strr_host}}) and compresses them into the
#' UPGo database storage format.
#'
#' A function for compressing daily activity tables. It takes either daily
#' listing activity tables (class `strr_daily`) or daily host activity tables
#' (class `strr_host`) and converts them into a more storage-efficient
#' one-activity-block-per-row format.
#'
#' The output can subsequently be restored to a non-compressed format using
#' \code{\link{strr_expand}}.
#'
#' @param data A daily table in either the processed UPGo daily format or the
#' processed UPGo host format.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A compressed daily table, ready for upload to a remote database.
#' @importFrom rlang .data
#' @export

strr_compress <- function(data, quiet = FALSE) {

  ### ERROR CHECKING AND INITIALIZATION ########################################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }

  # Check if table is daily or host
  if (inherits(data, "strr_daily") | names(data)[1] == "property_ID") {

    helper_message("Daily table identified.")

    daily <- TRUE

  } else if (inherits(data, "strr_host") | names(data)[1] == "host_ID") {

    helper_message("Host table identified.")

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_host`.")


  ## Define default versions of map_* ------------------------------------------

  map <- purrr::map
  map_dfr <- purrr::map_dfr


  ## Prepare data.table and future variables -----------------------------------

  .datatable.aware = TRUE

  # Silence R CMD check for data.table fields
  property_ID <- month <- PID_split <- host_split <- host_ID <- start_date <-
    NULL

  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    # Replace map_* with future_map_*
    map <- furrr::future_map
    map_dfr <- furrr::future_map_dfr

    # Remove limit on globals size
    options(future.globals.maxSize = +Inf)

    # Set up on.exit expression for errors
    on.exit({
      # Flush out any stray multicore processes
      map(1:future::nbrOfWorkers(), ~.x)

      # Restore future global export limit
      .Options$future.globals.maxSize <- NULL

    })
  }


  ## Prepare progress reporting ------------------------------------------------

  # Enable progress bars if quiet == FALSE
  progress <- !quiet

  # Disable progress bars if {progressr} is not installed
  if (!requireNamespace("progressr", quietly = TRUE)) {
    progress <- FALSE
    .strr_env$pb <- function() NULL
  }

  # Set number of steps for progress reporting
  steps <- 2


  ### PREPARE FILE FOR ANALYSIS ################################################

  ## Convert to data.table -----------------------------------------------------

  data.table::setDT(data)


  ## Store invariant fields for later ------------------------------------------

  if (daily) {

    join_cols <-
      c("host_ID", "listing_type", "housing", "country", "region", "city")

    join_fields <- data[, .SD[1L], by = property_ID, .SDcols = join_cols]

    data[, (join_cols) := NULL]

  }


  ## If data spans multiple months, produce month/year columns -----------------

  if (data.table::year(min(data$date)) != data.table::year(max(data$date))) {

    steps <- 3

    helper_message("(1/", steps, ") Adding year and month fields.",
                   .type = "open")

    data[, c("month", "year") := list(data.table::month(date),
                                      data.table::year(date))]

    helper_message("(1/", steps, ") Year and month fields added.",
                    .type = "close")

  } else if (data.table::month(min(data$date)) !=
             data.table::month(max(data$date))) {

    steps <- 3

    helper_message("(1/", steps, ") Adding month field.", .type = "open")

    data[, month := data.table::month(date)]

    helper_message("(1/", steps, ") Month field added.", .type = "close")

  }


  ### SPLIT TABLE FOR PROCESSING ###############################################

  ## Split by first three digits of property_ID/host_ID ------------------------

  if (daily) {

    data[, PID_split := substr(property_ID, 1, 6)]

    data_list <- split(data, by = "PID_split", keep.by = FALSE)
    data_list <- helper_table_split(data_list)

  } else {

    data[, host_split := substr(host_ID, 1, 3)]

    data_list <- split(data, by = "host_split", keep.by = FALSE)
    data_list <- helper_table_split(data_list)
  }


  ### COMPRESS PROCESSED DATA FILE #############################################

  helper_message("(", steps - 1, "/", steps,
                 ") Compressing rows, using {helper_plan()}.")


  ## Method with progress ------------------------------------------------------

  if (progress) {

    handler_strr("Compressing row")

    progressr::with_progress({

      .strr_env$pb <- progressr::progressor(steps = nrow(data))

      if (daily)  compressed <- map_dfr(data_list, helper_compress_daily)
      if (!daily) compressed <- map_dfr(data_list, helper_compress_host)

      })


  ## Method without progress ---------------------------------------------------

    } else {

      if (daily)  compressed <- map_dfr(data_list, helper_compress_daily)
      if (!daily) compressed <- map_dfr(data_list, helper_compress_host)

      }


  ## Add other columns to daily file -------------------------------------------

  # The join is faster and less memory-intensive with dplyr than data.table
  if (daily) compressed <-
    dplyr::left_join(compressed, join_fields, by = "property_ID")


  ### ARRANGE OUTPUT AND SET CLASS #############################################

  helper_message("(", steps, "/", steps,
                          ") Arranging output table.",
                          .type = "open")

  data.table::setDT(compressed)

  if (daily) {

    compressed <- dplyr::as_tibble(compressed[order(property_ID, start_date)])
    class(compressed) <- append(class(compressed), "strr_daily")

    } else {

      compressed <- dplyr::as_tibble(compressed[order(host_ID, start_date)])
      class(compressed) <- append(class(compressed), "strr_host")

      }

  helper_message("(", steps, "/", steps, ") Output table arranged.",
                 .type = "close")


  ### RETURN OUTPUT ############################################################

  helper_message("Compression complete.", .type = "final")

  # Overwrite previous on.exit call
  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    on.exit(.Options$future.globals.maxSize <- NULL)

  }

  return(compressed)
}


#' Helper function to compress daily file
#'
#' \code{helper_compress_daily} takes a processed `daily` table and
#' generates a compressed version.
#'
#' A helper function for compressing the processed monthly `daily` table.
#'
#' @param data The processed daily table generated through the strr_compress
#' function.
#' @return The output will be a compressed daily table.
#' @importFrom rlang .data

helper_compress_daily <- function(data) {

  # Iterate progress bar
  .strr_env$pb(amount = nrow(data))

  # Silence R CMD check for data.table fields
  booked_date <- dates <- end_date <- price <- property_ID <- res_ID <-
    start_date <- status <- NULL

  # Make sure data.table is single-threaded within the helper
  threads <- data.table::setDTthreads(1)
  on.exit(data.table::setDTthreads(threads))
  data.table::setDT(data)

  # Group data by all columns except date
  data <- data[, .(dates = list(date)), by = setdiff(names(data), "date")]

  # Find groupings with a single date
  one_length <- data[sapply(dates, function(x) {
    length(x) - length(min(x):max(x))}) == 0]

  if (nrow(one_length) > 0) {

    one_length <-
      one_length[, .(
        property_ID,
        start_date = as.Date(sapply(dates, min), origin = "1970-01-01"),
        end_date   = as.Date(sapply(dates, max), origin = "1970-01-01"),
        status, booked_date, price, res_ID)]

  } else {

    one_length <-
      data.table::data.table(
        property_ID = character(),
        start_date = as.Date(character()),
        end_date = as.Date(character()),
        status = character(),
        booked_date = as.Date(character()),
        price = integer(),
        res_ID = integer())

  }

  # More complex case where rows have a non-continuous date range
  if (nrow(data[sapply(dates, function(x) {
    length(x) - length(min(x):max(x))
    }) != 0,]) > 0) {

    remainder <-
      data[sapply(dates, function(x) {length(x) - length(min(x):max(x))}) != 0,
           .(property_ID,
             start_date = lapply(dates,
                                 function (x) {x[which(diff(c(0, x)) > 1)]}),
             end_date = lapply(dates,
                               function (x) {x[which(diff(c(x, 30000)) > 1)]}),
             status, booked_date, price, res_ID)]

    remainder <-
      remainder[, .(property_ID,
                    start_date = as.Date(unlist(start_date),
                                         origin = "1970-01-01"),
                    end_date = as.Date(unlist(end_date), origin = "1970-01-01"),
                    status, booked_date, price, res_ID),
                by = 1:nrow(remainder)]

    remainder[, nrow := NULL]

  } else remainder <- one_length[0,]

  data.table::rbindlist(list(one_length, remainder))
}


#' Helper function to compress host file
#'
#' \code{helper_compress_host} takes a processed host table and generates a
#' compressed version.
#'
#' A helper function for compressing the processed host summary table.
#'
#' @param data The processed host table generated through the strr_compress
#' function.
#' @return The output will be a compressed host table.
#' @importFrom rlang .data

helper_compress_host <- function(data) {

  # Silence R CMD check for data.table fields
  host_ID <- listing_type <- housing <- count <- dates <- start_date <-
    end_date <- NULL

  # Make sure data.table is single-threaded within the helper
  threads <- data.table::setDTthreads(1)
  on.exit(data.table::setDTthreads(threads))
  data.table::setDT(data)

  # Group data by all columns except date
  data <- data[, .(dates = list(date)), by = setdiff(names(data), "date")]

  # Simple compression for rows with a continuous date range
  one_length <-
    data[sapply(dates, function(x) {length(x) - length(min(x):max(x))}) == 0,]

  if (nrow(one_length) > 0) {

    one_length <-
      one_length[, .(
        host_ID,
        start_date = as.Date(sapply(dates, min), origin = "1970-01-01"),
        end_date   = as.Date(sapply(dates, max), origin = "1970-01-01"),
        listing_type, housing, count)]

  } else {

    one_length <-
      data.table::data.table(
        host_ID = character(),
        start_date = as.Date(character()),
        end_date = as.Date(character()),
        listing_type = character(),
        housing = logical(),
        count = integer())
  }

  # More complex case where rows have a non-continuous date range
  if (nrow(data[sapply(dates, function(x) {
    length(x) - length(min(x):max(x))
    }) != 0,]) > 0) {

    remainder <-
      data[sapply(dates, function(x) {length(x) - length(min(x):max(x))}) != 0,
           .(host_ID,
             start_date = lapply(dates,
                                 function (x) {x[which(diff(c(0, x)) > 1)]}),
             end_date = lapply(dates,
                               function (x) {x[which(diff(c(x, 30000)) > 1)]}),
             listing_type, housing, count)]

    remainder <-
      remainder[, .(host_ID,
                    start_date = as.Date(unlist(start_date),
                                         origin = "1970-01-01"),
                    end_date = as.Date(unlist(end_date), origin = "1970-01-01"),
                    listing_type, housing, count),
                by = 1:nrow(remainder)]

    remainder[, nrow := NULL]

  } else remainder <- one_length[0,]

  data.table::rbindlist(list(one_length, remainder))
}

