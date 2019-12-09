#' Function to identify frequently rented entire-home (FREH) listings
#'
#' \code{strr_freh} takes a table of daily STR activity and identifies listings
#' which met a given standard of availability and activity over a specified
#' time period.
#'
#' A function for identifying clusters of possible "ghost hostels"--clusters of
#' private-room STR listings operating in a single building. The function works
#' by intersecting the possible locations of listings operated by a single host
#' with each other, to find areas which could the common location of the
#' listings, and thus be one or more housing units subdivided into private rooms
#' rather than a set of geographically disparate listings. The function can
#' optionally run its analysis separately for each date within a time period,
#' and can also check for possible duplication with entire-home listings
#' operated by the same host.
#'
#' @param .daily A data frame of daily STR activity in standard UPGo format.
#' @param start_date A character string of format YYYY-MM-DD indicating the
#'   first date for which to return output. If NULL (default), all dates will
#'   be used. This argument is ignored if `multi_date` is FALSE.
#' @param end_date A character string of format YYYY-MM-DD indicating the last
#'   date for which to run the analysis. If NULL (default), all dates will be
#'   used. This argument is ignored if `multi_date` is FALSE.
#' @param property_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR listings.
#' @param date TKTK
#' @param status TKTK
#' @param status_types TKTK
#' @param listing_type The name of a character variable in the `.daily``
#'   object which identifies entire-home listings. Set this argument to FALSE
#'   to use all listings in the `.daily` table.
#' @param entire_home A character string which identifies the value of the
#'   `listing_type` variable to be used to find entire-home listings. This field
#'   is ignored if `listing_type` is FALSE.
#' @param n_days TKTK
#' @param r_cut TKTK
#' @param ar_cut TKTK
#' @param cores A positive integer scalar. How many processing cores should be
#'   used to perform the computationally intensive steps?
#' @param quiet A logical vector. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be a tidy data frame of identified FREH listings,
#'   organized with the following fields: `property_ID` (or whatever name was
#'   passed to the property_ID argument): A character vector with the ID code of
#'   the listings. `date`: The date for which the FREH status is being reported.
#'   `FREH`: A logical scalar indicating whether, on a given date, the given
#'   listing exceeded the `r_cut` and `ar_cut` thresholds over the number of
#'   days specified by `n_days`.
#' @importFrom data.table rbindlist
#' @importFrom dplyr %>% as_tibble filter rename
#' @importFrom rlang .data
#' @export

strr_FREH <- function(.daily, start_date, end_date, property_ID = property_ID,
                      date = date, status = status, status_types = c("R", "A"),
                      listing_type = listing_type,
                      entire_home = "Entire home/apt", n_days = 365, r_cut = 90,
                      ar_cut = 183, cores = 1, quiet = FALSE) {

  .datatable.aware = TRUE

  # Define local variables to avoid R CMD check notes
  R <- NULL
  AR <- NULL

  # Wrangle dates
  start_date <- as.Date(start_date, origin = "1970-01-01")
  end_date <- as.Date(end_date, origin = "1970-01-01")

  # Filter daily file
  .daily <-
    .daily %>%
    filter({{ status }} %in% c("A", "R"), {{ date }} >= start_date - 364,
           {{ date }} <= end_date, {{ listing_type }} == entire_home)

  # Rename fields to make data.table functions work
  .daily <-
    .daily %>%
    rename(property_ID = {{ property_ID }},
           date = {{ date }},
           status = {{ status }},
           listing_type = {{ listing_type }})

  # Perform calculations

  setDT(.daily)

  .daily <-
    pbapply::pblapply(start_date:end_date, function(date_check) {
      .daily <- .daily[date >= date_check - 364 & date <= date_check]
      .daily[, AR := .N, by = property_ID]
      .daily[, R := sum(status == "R"), by = property_ID]
      .daily[, list(date = as.Date(date_check, origin = "1970-01-01"),
                   FREH = as.logical((mean(AR) >= ar_cut) *
                                       (mean(R) >= r_cut))),
            by = property_ID]
    }, cl = cores) %>%
      rbindlist() %>%
      as_tibble()

  # Rename fields to match input fields
  .daily <-
    .daily %>%
    rename({{ property_ID }} := .data$property_ID,
           {{ date }} := .data$date)

  .daily

}
