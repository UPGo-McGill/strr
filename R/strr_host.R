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

  helper_check_daily()
  helper_check_quiet()


  ## Prepare data.table and future variables -----------------------------------

  # Silence R CMD check for data.table fields
  host_ID <- status <- date <- listing_type <- housing <- host_split <- .GRP <-
    NULL

  if (requireNamespace("future", quietly = TRUE)) {

    # Set data.table threads to match future workers
    threads <- data.table::setDTthreads(future::nbrOfWorkers())

    # Restore data.table threads on exit
    on.exit(data.table::setDTthreads(threads))

  }


  ### TRIM DAILY TABLE #########################################################

  helper_message("(1/2) Trimming daily table to valid entries.", .type = "open")

  data.table::setDT(daily)

  daily <- daily[!is.na(host_ID), .(host_ID, date, listing_type, housing)]

  # Save nrow for final validity check
  daily_check <- nrow(daily)

  helper_message("(1/2) Daily table trimmed to valid entries.", .type = "close")


  ### PRODUCE HOST TABLE #######################################################

  helper_message("(2/2) Analyzing rows, using ", helper_plan(), ".")

  # Use future assignment if plan is remote
  host %<-% daily[,.(count = .N), by = .(host_ID, date, listing_type, housing)]

  host <- dplyr::as_tibble(host)


  ### CHECK AND RETURN OUTPUT ##################################################

  # Check validity of output
  if (daily_check != sum(host$count)) {
    stop("The function did not return the correct number of entries. ",
         "This might be because a parallel worker failed to complete its job.")
  }

  helper_message("Processing complete.", .type = "final")

  return(host)
}
