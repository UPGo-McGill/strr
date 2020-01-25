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
#' @importFrom data.table month setDT year
#' @importFrom dplyr %>% arrange bind_rows filter group_by group_split mutate
#' @importFrom dplyr pull select
#' @importFrom furrr future_map_dfr
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_compress <- function(data, quiet = FALSE) {

  time_1 <- Sys.time()


  ### Error checking and initialization ########################################


  ## Set data.table variables

  .datatable.aware = TRUE

  property_ID <- start_date <- host_ID <- PID_split <- host_split <- NULL


  ## Remove future global export limit

  options(future.globals.maxSize = +Inf)

  on.exit(.Options$future.globals.maxSize <- NULL)

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }

  # Check if table is daily or host

  if (inherits(data, "strr_daily") | names(data)[1] == "property_ID") {

    helper_progress_message("Daily table identified.")

    daily <- TRUE

  } else if (inherits(data, "strr_host") | names(data)[1] == "host_ID") {

    helper_progress_message("Host table identified.")

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_host`.")


  ### Prepare file for analysis ################################################

  ## Convert to data.table

  setDT(data)


  ## Store invariant fields for later

  if (daily) {

    join_cols <-
      c("host_ID", "listing_type", "housing", "country", "region", "city")

    #### TKTK DELETE ONCE DB IS UPDATED
    if (length(daily) == 14) {
      join_cols <-
        c("host_ID", "listing_type", "created", "scraped", "housing", "country",
          "region", "city")
    }
    #### TKTK DELETE TO HERE


    join_fields <-
      data[, .SD[1L], by = property_ID,
           .SDcols = join_cols]

    data[, (join_cols) := NULL]

  }


  ## If data spans multiple months, produce month/year columns

  if (year(min(data$date)) != year(max(data$date))) {

    helper_progress_message("Adding year and month fields.")

    data[, c("month", "year") := list(month(date), year(date))]

  } else if (month(min(data$date)) != month(max(data$date))) {

    helper_progress_message("Adding month field.")

    data[, month := month(date)]

  }


  ### Split table for processing ###############################################

  ## Split by first three digits of property_ID/host_ID

  helper_progress_message("Splitting table for processing.")

  if (daily) {

    data[, PID_split := substr(property_ID, 1, 6)]

    data_list <-
      split(data, by = "PID_split", keep.by = FALSE) %>%
      helper_table_split()

    # Use host_ID for host
  } else {

    data[, host_split := substr(host_ID, 1, 3)]

    data_list <-
      split(data, by = "host_split", keep.by = FALSE) %>%
      helper_table_split()
  }


  ### Compress processed data file #############################################

  helper_progress_message("Beginning compression, using {helper_plan()}.")

  if (daily) {

    compressed <-
      data_list %>%
      future_map_dfr(strr_compress_helper,
                     # Suppress progress bar if !quiet or the plan is remote
                     .progress = helper_progress()) %>%
      # The join is faster and less memory-intensive with dplyr than data.table
      left_join(join_fields, by = "property_ID")

  } else {

    compressed <-
      data_list %>%
      future_map_dfr(strr_compress_helper_host,
                     # Suppress progress bar if !quiet or the plan is remote
                     .progress = helper_progress())
  }


  ## Arrange output and set class

  helper_progress_message("Arranging output table.")

  setDT(compressed)

  if (daily) {
    compressed <- as_tibble(compressed[order(property_ID, start_date)])
    class(compressed) <- append(class(compressed), "strr_daily")
    } else {
      compressed <- as_tibble(compressed[order(host_ID, start_date)])
      class(compressed) <- append(class(compressed), "strr_host")
      }


  ## Return output

  helper_progress_message("Compression complete.", .final = TRUE)

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
#' @importFrom data.table rbindlist setDT
#' @importFrom rlang .data

strr_compress_helper <- function(data) {

  # Silence R CMD check for data.table fields
  booked_date <- dates <- end_date <- price <- property_ID <- res_ID <-
    start_date <- status <- NULL

  setDT(data)

  # Group data by all columns except date
  data <- data[, .(dates = list(date)), by = setdiff(names(data), "date")]

  one_length <-
    data[sapply(dates, function(x) {length(x) - length(min(x):max(x))}) == 0,
         .(property_ID,
           start_date = as.Date(sapply(dates, min), origin = "1970-01-01"),
           end_date   = as.Date(sapply(dates, max), origin = "1970-01-01"),
           status, booked_date, price, res_ID)]

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

  rbindlist(list(one_length, remainder))
}


#' Helper function to compress host file
#'
#' \code{strr_compress_helper_host} takes a processed host table and generates a
#' compressed version.
#'
#' A helper function for compressing the processed host summary table.
#'
#' @param data The processed host table generated through the strr_compress
#' function.
#' @return The output will be a compressed host table.
#' @importFrom data.table rbindlist setDT
#' @importFrom rlang .data

strr_compress_helper_host <- function(data) {

  # Silence R CMD check for data.table fields
  host_ID <- listing_type <- housing <- count <- dates <- start_date <-
    end_date <- NULL

  setDT(data)

  # Group data by all columns except date
  data <- data[, .(dates = list(date)), by = setdiff(names(data), "date")]

  # Simple compression for rows with a continuous date range
  one_length <-
    data[sapply(dates, function(x) {length(x) - length(min(x):max(x))}) == 0,
         .(host_ID,
           start_date = as.Date(sapply(dates, min), origin = "1970-01-01"),
           end_date   = as.Date(sapply(dates, max), origin = "1970-01-01"),
           listing_type, housing, count)]

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

  rbindlist(list(one_length, remainder))
}

