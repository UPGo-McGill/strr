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
#' @importFrom data.table setDT setDTthreads
#' @importFrom dplyr %>%
#' @importFrom future %<-% nbrOfWorkers
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_host <- function(daily, quiet = FALSE) {

  ### Error checking and initialization ########################################

  time_1 <- Sys.time()


  ## data.table and future setup

  .datatable.aware = TRUE
  host_ID <- status <- date <- listing_type <- housing <- host_split <- .GRP <-
    NULL
  options(future.globals.maxSize = +Inf)

  ## Set up on.exit expression for errors

  on.exit({
    # Restore future global export limit
    .Options$future.globals.maxSize <- NULL

    # Print \n so error messages don't collide with progress messages
    if (!quiet) message()
  })



  ## Input checking

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

  helper_progress_message("Daily table identified.")


  ### Trim daily table #########################################################

  helper_progress_message("(1/3) Trimming daily table to valid entries.",
                          .type = "open")

  setDT(daily)

  daily <-
    daily[status != "U" & !is.na(host_ID),
          .(host_ID, date, listing_type, housing)]

  # Save nrow for final validity check
  daily_check <- nrow(daily)

  helper_progress_message("(1/3) Daily table trimmed to valid entries.",
                          .type = "close")


  ### Produce multilisting table ###############################################

  helper_progress_message("(2/2) Beginning processing, using {helper_plan()}.",
                          .type = "progress")

  # Only use future assignment if plan is remote
  if ("remote" %in% class(future::plan())) {
    host %<-% {
      setDTthreads(future::nbrOfWorkers())

      daily[,.(count = .N), by = .(host_ID, date, listing_type, housing)] %>%
        as_tibble()
    }
  } else {
    setDTthreads(future::nbrOfWorkers())

    host <-
      daily[,.(count = .N), by = .(host_ID, date, listing_type, housing)] %>%
      as_tibble()

  }


  ### Check and return output ##################################################

  ## Check validity of output

  if (daily_check != sum(host$count)) {
    stop("The function did not return the correct number of entries. ",
         "This might be because a parallel worker failed to complete its job.")
  }


  ## Return output

  class(host) <- append(class(host), "strr_host")

  helper_progress_message("Processing complete.", .type = "final")

  # Overwrite previous on.exit call
  on.exit(.Options$future.globals.maxSize <- NULL)

  return(host)
}
