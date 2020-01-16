#' Function to create daily multilisting tables
#'
#' \code{strr_process_multi} takes processed daily tables and produces summary
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
#' @importFrom data.table setDT
#' @importFrom dplyr %>% group_split
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_process_multi <- function(daily, quiet = FALSE) {

  time_1 <- Sys.time()

  ### Error checking and initialization ########################################

  ## data.table setup

  .datatable.aware = TRUE

  host_ID <- status <- date <- listing_type <- housing <- NULL


  ## Error checking

  # Remove future global export limit
  options(future.globals.maxSize = +Inf)
  on.exit(.Options$future.globals.maxSize <- NULL)

  # Check that table is a data frame
  if (!inherits(daily, "data.frame")) {
    stop("The object supplied to the `daily` argument must be a data frame.")
  }

  # Check that table is daily
  if (!inherits(daily, "strr_multi") | names(daily)[1] != "property_ID") {
    stop("Input table must be of class `strr_daily`.")
  }

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }


  ### Trim daily table #########################################################

  helper_progress_message("Trimming daily table to valid entries.")

  setDT(daily)

  daily <-
    daily[status != "U" & !is.na(host_ID),
          .(host_ID, date, listing_type, housing)]

  # Save nrow for final validity check
  daily_check <- nrow(daily)


  ## Produce list for processing

  data_list <-
    daily %>%
    group_split(.data$host_ID) %>%
    helper_table_split()


  ### Produce multilisting table ###############################################

  helper_progress_message("Beginning processing, using {helper_plan()}.")

  multi <-
    data_list %>%
    future_map_dfr(~{
      setDT(.x)
      .x[,.(count = .N), by = .(host_ID, date, listing_type, housing)] %>%
        as_tibble()
    },
    # Suppress progress bar if quiet == TRUE or the plan is remote
    .progress = helper_progress(quiet))


  ## Check validity of output

  if (daily_check != sum(multi$count)) {
    stop("The function did not return the correct number of entries. ",
         "This might be because a parallel worker failed to complete its job.")
  }


  ## Return output

  class(multi) <- append(class(multi), "strr_multi")

  helper_progress_message("Processing complete.", .final = TRUE)

  return(multi)
}
