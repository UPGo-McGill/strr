#' Function to create daily host activity tables
#'
#' \code{strr_host} takes processed daily tables and produces summary
#' tables listings host activity per day.
#'
#' A function for aggregating daily activity tables by host, listing type, and
#' housing status, in order to determine the extent of multilisting activity
#' in a given STR dataset. This function will typically be run on the output of
#' \code{\link{strr_process_daily}} in preparation for compression with
#' \code{\link{strr_compress}}.
#'
#' @param daily An processed daily table in the UPGo format (probably produced
#' with \code{\link{strr_process_daily}}), with either ten or six fields.
#' @param quiet A logical scalar. Should the function execute quietly, or
#' should it return status updates throughout the function (default)?
#' @return A processed multilisting table, ready for compression with
#' \code{\link{strr_compress}}.
#' @export

strr_host <- function(daily, quiet = FALSE) {

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  # Check that table is a data frame
  if (!inherits(daily, "data.frame")) {
    stop("The object supplied to the `daily` argument must be a data frame.")
  }

  # Check that table is daily
  if (!inherits(daily, "strr_daily") & names(daily)[1] != "property_ID") {
    stop("Input table must be of class `strr_daily`.")
  }

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }


  ## Prepare data.table and future variables -----------------------------------

  .datatable.aware = TRUE

  # Silence R CMD check for data.table fields
  host_ID <- status <- date <- listing_type <- housing <- host_split <- .GRP <-
    NULL

  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    # Remove limit on globals size
    options(future.globals.maxSize = +Inf)

    # Set data.table threads to match future workers
    threads <- data.table::setDTthreads(future::nbrOfWorkers())

    # Set up on.exit expression for errors
    on.exit({
      # Flush out any stray multicore processes
      furrr::future_map(1:future::nbrOfWorkers(), ~.x)

      # Restore future global export limit
      .Options$future.globals.maxSize <- NULL

      # Restore data.table threads
      data.table::setDTthreads(threads)

    })
  }


  ### TRIM DAILY TABLE #########################################################

  helper_message("(1/2) Trimming daily table to valid entries.", .type = "open")

  data.table::setDT(daily)

  daily <- daily[!is.na(host_ID), .(host_ID, date, listing_type, housing)]

  # Save nrow for final validity check
  daily_check <- nrow(daily)

  helper_message("(1/2) Daily table trimmed to valid entries.", .type = "close")


  ### PRODUCE HOST TABLE #######################################################

  helper_message("(2/2) Analyzing rows, using {helper_plan()}.")

  host <- daily[,.(count = .N), by = .(host_ID, date, listing_type, housing)]
  host <- dplyr::as_tibble(host)


  ### CHECK AND RETURN OUTPUT ##################################################

  # Check validity of output
  if (daily_check != sum(host$count)) {
    stop("The function did not return the correct number of entries. ",
         "This might be because a parallel worker failed to complete its job.")
  }

  # Set class
  class(host) <- append(class(host), "strr_host")

  # Overwrite previous on.exit call
  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    on.exit({
      .Options$future.globals.maxSize <- NULL
      data.table::setDTthreads(threads)

    })
  }

  helper_message("Processing complete.", .type = "final")

  return(host)
}
