#' Identify STR multilistings
#'
#' \code{strr_multi} takes a daily table and a host table and identifies which
#' listings in the daily table are multilistings on each date.
#'
#' The function summarizes the listings operated by a given host on a given date
#' to determine if the listings are "multilistings"--i.e. more listings operated
#' by a single host than is consistent with the host being a home sharer.
#'
#' @param daily A data frame of daily STR activity in standard UPGo format.
#' @param host A data frame of STR host activity in standard UPGo format.
#' @param thresholds An integer vector indicating the thresholds to establish
#' multilisting status for each listing type. If the vector is unnamed, the
#' options will be read in the order "Entire home/apt", "Private room",
#' "Shared room", "Hotel room". If the vector is named, using the full listing
#' type names ("Entire home/apt", "Private room", "Shared room", "Hotel room")
#' or acronyms ("EH", "PR", "SR", "HR"), the values can be presented in any
#' order. An NA or 0 value means that multilisting calculations will not be
#' performed for that listing type.
#' @param combine_listing_type A logical scalar. Should multilisting status for
#' one listing type (e.g. "Entire home/apt") confer multilisting status on the
#' other listing types operated by a host on a given day, regardless of whether
#' the other listing types exceeded the multilisting threshold (default)?
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
#' @export

strr_multi <- function(daily, host,
                       thresholds = c(EH = 2L, PR = 3L, SR = NA, HR = NA),
                       combine_listing_type = TRUE,
                       combine_housing = TRUE, field_name = multi,
                       quiet = FALSE) {

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  helper_check_daily()
  helper_check_host()
  helper_check_quiet()

  stopifnot(is.logical(combine_listing_type), is.logical(combine_housing))


  ## Prepare data.table and future variables -----------------------------------

  # Silence R CMD check for data.table fields
  listing_type <- .ML <- count <- NULL

  if (requireNamespace("future", quietly = TRUE)) {

    # Set data.table threads to match future workers
    threads <- data.table::setDTthreads(future::nbrOfWorkers())

    # Restore data.table threads on exit
    on.exit(data.table::setDTthreads(threads))

  }


  ### CHECK THRESHOLDS ARGUMENTS ###############################################

  # Store names
  thresholds_names <- names(thresholds)

  # Replace 0 with NA
  thresholds <- dplyr::if_else(thresholds == 0, NA_integer_,
                               as.integer(thresholds))

  # Calculate steps for progress reporting
  steps <- sum(!is.na(thresholds), combine_housing) + 1
  steps_so_far <- 0


  ### PREPARE TABLES ###########################################################

  data.table::setDT(daily)
  data.table::setDT(host)

  col_names <- names(daily)


  ### EXTRACT THRESHOLDS FROM VECTOR ###########################################

  EH <- PR <- SR <- HR <- NA

  if (is.null(thresholds_names)) {
    EH <- thresholds[1]
    PR <- thresholds[2]
    SR <- thresholds[3]
    HR <- thresholds[4]

  } else{

    EH <- thresholds[thresholds_names %in% c("EH", "Entire home/apt")]
    PR <- thresholds[thresholds_names %in% c("PR", "Private room")]
    SR <- thresholds[thresholds_names %in% c("SR", "Shared room")]
    HR <- thresholds[thresholds_names %in% c("HR", "Hotel room")]

  }


  ### COMBINE LISTINGS BY HOUSING FIELD ########################################

  if (combine_housing) {

    steps_so_far <- steps_so_far + 1

    helper_message(
      "(", steps_so_far, "/", steps,
      ") Combining housing and non-housing listings, using ", helper_plan(),
      ".", .type = "open")

    # Use future assignment if plan is remote
    host %<-%
      host[, .(count = sum(count)), by = c("host_ID", "date", "listing_type")]

  }

  helper_message(
    "(", steps_so_far, "/", steps,
    ") Housing and non-housing listings combined, using ", helper_plan(), ".",
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
                 ") Joining results into daily table, using ", helper_plan(),
                 ".", .type = "open")

  # Establish columns to join on
  join_cols <- setdiff(names(multi), ".ML")


  ## Merge results if combine_listing_type == TRUE -----------------------------

  if (combine_listing_type) {

    multi <-
      multi[, .(.ML = sum(.ML)), by = c("host_ID", "date")
            ][,.ML := as.logical(.ML)]

    join_cols <- setdiff(names(multi), c(".ML", "listing_type"))

  }


  ## Perform join --------------------------------------------------------------

  # Use future assignment if plan is remote
  daily %<-% multi[daily, on = join_cols
                     ][, .ML := dplyr::if_else(is.na(.ML), FALSE, .ML)]

  helper_message("(", steps_so_far, "/", steps,
                 ") Results joined into daily table, using ", helper_plan(),
                 ".", .type = "close")


  ## Clean up ------------------------------------------------------------------

  data.table::setcolorder(daily, c(col_names, ".ML"))

  daily <- dplyr::as_tibble(daily)
  daily  <- dplyr::rename(daily, {{field_name}} := .data$.ML)


  ### RETURN OUTPUT ############################################################

  helper_message("Processing complete.", .type = "final")

  return(daily)

}
