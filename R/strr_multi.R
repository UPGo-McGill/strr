#' Function to identify STR multilistings
#'
#' \code{strr_multi} takes a daily table and a host table and identifies which
#' listings in the daily table are multilistings on each date.
#'
#' TKTK explanation
#'
#' @param daily A data frame of daily STR activity in standard UPGo format.
#' @param host A data frame of STR host activity in standard UPGo format.
#' @param thresholds An integer vector indicating the thresholds to establish
#' multilisting status for each listing type. If the vector is unnamed, the
#' options will be read in the order "Entire home/apt", "Private room",
#' "Shared room", "Hotel room". If the vector is named, using the full listing
#' type names or acronyms ("EH", "PR", "SR", "HR"), the values can be presented
#' in any order. An NA or 0 value means that multilisting calculations will not
#' be performed for that listing type.
#' @param combine_housing A logical scalar. Should housing and non-housing
#' listings be combined for the purposes of establishing multilisting status
#' (default)?
#' @param field_name A symbol or character string to name the new logical field
#' identifying multilisting status in the output table. The default is
#' `multi`.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be the `daily` input table with one additional
#' logical field (with name taken from the `field_name` argument) indicating
#' multilisting status.
#' @importFrom rlang .data
#' @export

strr_multi <- function(daily, host,
                       thresholds = c(EH = 2L, PR = 3L, SR = NA, HR = NA),
                       combine_housing = TRUE, field_name = multi,
                       quiet = FALSE) {

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  # Check that daily is a data frame
  if (!inherits(daily, "data.frame")) {
    stop("The object supplied to the `daily` argument must be a data frame.")
  }

  # Check that daily is of class strr_daily
  if (!inherits(daily, "strr_daily") & names(daily)[1] != "property_ID") {
    stop("The object supplied to the `daily` argument ",
         "must be of class `strr_daily`.")
  }

  # Check that host is a data frame
  if (!inherits(host, "data.frame")) {
    stop("The object supplied to the `host` argument must be a data frame.")
  }

  # Check that host is of class strr_host
  if (!inherits(host, "strr_host") & names(host)[1] != "host_ID") {
    stop("The object supplied to the `host` argument ",
         "must be of class `strr_host`.")
  }

  # Check that combine_housing is a logical
  if (!is.logical(combine_housing)) {
    stop("The argument `combine_housing` ",
         "must be a logical value (TRUE or FALSE).")
  }

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }


  ## Prepare data.table and future variables -----------------------------------

  .datatable.aware = TRUE

  # Silence R CMD check for data.table fields
  listing_type <- .ML <- NULL

  # Default to local analysis
  remote <- FALSE

  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    # Remove limit on globals size
    options(future.globals.maxSize = +Inf)

    # Set data.table threads to match future workers
    threads <- data.table::setDTthreads(future::nbrOfWorkers())

    # Prepare for remote execution
    if ("remote" %in% class(future::plan())) {
      remote <- TRUE
      `%<-%` <- future::`%<-%`()
    }

    # Set up on.exit expression for errors
    on.exit({
      # Flush out any stray multicore processes
      furrr::future_map(1:future::nbrOfWorkers(), ~.x)

      # Restore future global export limit
      .Options$future.globals.maxSize <- NULL

      # Restore data.table threads
      data.table::setDTthreads(threads)

      # Print \n so error messages don't collide with progress messages
      if (!quiet) message()

    })
  }


  ### CHECK THRESHOLDS ARGUMENTS ###############################################

  # Coerce to integer
  thresholds <- as.integer(thresholds)

  # Replace 0 with NA
  thresholds <- dplyr::if_else(thresholds == 0, NA_integer_, thresholds)

  # Calculate steps for progress reporting
  steps <- sum(!is.na(thresholds), combine_housing) + 1
  steps_so_far <- 0


  ### PREPARE TABLES ###########################################################

  data.table::setDT(daily)
  data.table::setDT(host)

  col_names <- names(daily)


  ### EXTRACT THRESHOLDS FROM VECTOR ###########################################

  EH <- PR <- SR <- HR <- NA

  if (is.null(names(thresholds))) {
    EH <- thresholds[1]
    PR <- thresholds[2]
    SR <- thresholds[3]
    HR <- thresholds[4]

  } else{

    EH <- thresholds[names(thresholds) %in% c("EH", "Entire home/apt")]
    PR <- thresholds[names(thresholds) %in% c("PR", "Private room")]
    SR <- thresholds[names(thresholds) %in% c("SR", "Shared room")]
    HR <- thresholds[names(thresholds) %in% c("HR", "Hotel room")]

  }


  ### COMBINE LISTINGS BY HOUSING FIELD ########################################

  if (combine_housing) {

    steps_so_far <- steps_so_far + 1

    helper_message(
      "(", steps_so_far, "/", steps,
      ") Combining housing and non-housing listings, using {helper_plan()}.",
      .type = "open")

    # Only use future assignment if plan is remote
    if (remote) {
      host %<-%
        host[, .(count = sum(count)), by = c("host_ID", "date", "listing_type")]

    } else {
      host <-
        host[, .(count = sum(count)), by = c("host_ID", "date", "listing_type")]
    }
  }

  helper_message(
    "(", steps_so_far, "/", steps,
    ") Housing and non-housing listings combined, using {helper_plan()}.",
    .type = "close")


  ### EXTRACT SUBSETS AND JOIN TO DAILY ########################################

  ## Entire home/apt -----------------------------------------------------------

  if (!is.na(EH)) {

    steps_so_far <- steps_so_far + 1

    helper_message("(", steps_so_far, "/", steps,
                   ") Calculating entire-home listings", .type = "open")

    multi <-
      host[listing_type == "Entire home/apt" & count >= EH
           ][, c(".ML", "count") := list(TRUE, NULL)]

    helper_message( "(", steps_so_far, "/", steps,
                    ") Entire-home listings calculated.", .type = "close")

    }


  ## Private room --------------------------------------------------------------

  if (!is.na(PR)) {

    steps_so_far <- steps_so_far + 1

    helper_message("(", steps_so_far, "/", steps,
                   ") Calculating private-room listings.", .type = "open")

    if (is.na(EH)) {
      multi <- host[listing_type == "Private room" & count >= PR
                    ][, c(".ML", "count") := list(TRUE, NULL)]

      } else {
        multi <- data.table::rbindlist(list(
          multi, host[listing_type == "Private room" & count >= PR
                      ][, c(".ML", "count") := list(TRUE, NULL)]))
      }

    helper_message("(", steps_so_far, "/", steps,
                   ") Private-room listings calculated.", .type = "close")
    }


  ## Shared room ---------------------------------------------------------------

  if (!is.na(SR)) {

    steps_so_far <- steps_so_far + 1

    helper_message("(", steps_so_far, "/", steps,
                   ") Calculating shared-room multilistings.", .type = "open")

    if (is.na(EH) && is.na(PR)) {
      multi <- host[listing_type == "Shared room" & count >= SR
                    ][, c(".ML", "count") := list(TRUE, NULL)]

    } else {
      multi <- data.table::rbindlist(list(
        multi, host[listing_type == "Shared room" & count >= SR
                    ][, c(".ML", "count") := list(TRUE, NULL)]))
    }

    helper_message("(", steps_so_far, "/", steps,
                   ") Shared-room multilistings calculated.", .type = "close")

    }


  ## Hotel room ----------------------------------------------------------------

  if (!is.na(HR)) {

    steps_so_far <- steps_so_far + 1

    helper_message("(", steps_so_far, "/", steps,
                   ") Calculating hotel-room multilistings.", .type = "open")

    if (is.na(EH) && is.na(PR) && is.na(SR)) {
      multi <- host[listing_type == "Hotel room" & count >= HR
                    ][, c(".ML", "count") := list(TRUE, NULL)]

    } else {
      multi <- data.table::rbindlist(list(
        multi, host[listing_type == "Hotel room" & count >= HR
                    ][, c(".ML", "count") := list(TRUE, NULL)]))
    }

    helper_message("(", steps_so_far, "/", steps,
                   ") Hotel-room multilistings calculated.", .type = "close")

    }


  ### JOIN RESULTS INTO DAILY TABLE ############################################

  steps_so_far <- steps_so_far + 1

  helper_message("(", steps_so_far, "/", steps,
                 ") Joining results into daily table, using {helper_plan()}.",
                 .type = "open")

  # Only use future assignment if plan is remote
  if (remote) {
    daily %<-% multi[daily, on = setdiff(names(multi), ".ML")
                     ][, .ML := if_else(is.na(.ML), FALSE, .ML)]

  } else {
    daily <- multi[daily, on = setdiff(names(multi), ".ML")
                   ][, .ML := if_else(is.na(.ML), FALSE, .ML)]
  }

  helper_message("(", steps_so_far, "/", steps,
                 ") Results joined into daily table, using {helper_plan()}.",
                 .type = "close")

  data.table::setcolorder(daily, c(col_names, ".ML"))

  daily <- dplyr::as_tibble(daily)
  daily  <- dplyr::rename(daily, {{ field_name }} := .data$.ML)

  class(daily) <- append(class(daily), "strr_daily")


  ### RETURN OUTPUT ############################################################

  # Overwrite previous on.exit call
  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    on.exit({
      .Options$future.globals.maxSize <- NULL
      data.table::setDTthreads(threads)

    })
  }

  helper_message("Processing complete.", .type = "final")

  return(daily)

}
