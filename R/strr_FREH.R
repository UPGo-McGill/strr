#' Function to identify frequently rented entire-home (FREH) listings
#'
#' \code{strr_FREH} takes a table of daily STR activity and identifies listings
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

  time_1 <- Sys.time()

  if (!quiet) message("Preparing table for analysis. (",
                      substr(Sys.time(), 12, 19), ")")


  ## Set data.table flag and define local variables to avoid R CMD check notes

  .datatable.aware = TRUE

  R <- NULL
  AR <- NULL


  ## Check n_days, r_cut, ar_cut, and cores arguments

  # Check that n_days is an integer > 0
  n_days <- floor(n_days)
  if (n_days <= 0) {
    stop("The argument `n_days` must be a positive integer.")
  }

  # Check that r_cut is an integer > 0 and <= n_days
  r_cut <- floor(r_cut)
  if (r_cut <= 0 | r_cut > n_days) {
    stop("The argument `r_cut` must be a positive integer less than `n_days`.")
  }

  # Check that ar_cut is an integer > 0, <= n_days and >= r_cut
  ar_cut <- floor(ar_cut)
  if (ar_cut <= 0 | ar_cut > n_days | ar_cut < r_cut) {
    stop(paste0("The argument `ar_cut` must be a positive integer less than ",
                "`n_days` and greater than `r_cut`."))
  }

  # Check that cores is an integer > 0
  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }


  ## Check status_types


  ## Check that .daily fields exist

  tryCatch(
    pull(.daily, {{ property_ID }}),
    error = function(e) {
      stop("The value of `property_ID` is not a valid field in the input table."
      )})

  tryCatch(
    pull(.daily, {{ date }}),
    error = function(e) {
      stop("The value of `date` is not a valid field in the input table."
      )})

  tryCatch(
    pull(.daily, {{ status }}),
    error = function(e) {
      stop("The value of `status` is not a valid field in the input table."
      )})

  ## Check that status_types arguments are plausible

  if (length(status_types) != 2) {
    stop("The `status_type` argument must be a vector of length 2.")
  }

  if (.daily %>% filter({{ status }} == status_types[1]) %>% nrow() == 0) {
    warning(paste0("The first supplied argument to `status_types` returns no ",
                   "matches in the input table. Are you sure the argument ",
                   "is correct?"))
  }

  if (.daily %>% filter({{ status }} == status_types[2]) %>% nrow() == 0) {
    warning(paste0("The first supplied argument to `status_types` returns no ",
                   "matches in the input table. Are you sure the argument ",
                   "is correct?"))
  }


  ## Set lt_flag and check validity of listing_type

  lt_flag <-
    tryCatch(
      {
        # If listing_type is a field in points, set lt_flag = TRUE
        pull(.daily, {{ listing_type }})
        TRUE
      },
      error = function(e) {
        tryCatch(
          {
            # If listing_type == FALSE, set lt_flag = FALSE
            if (!listing_type) { FALSE
            } else stop("`listing_type` must be a valid field name or FALSE.")
          },
          error = function(e2) {
            # Otherwise, fail with an informative error
            stop("`listing_type` must be a valid field name or FALSE.")
          }
        )
      }
    )

  ## Check entire_home arguments

  if (lt_flag) {

    if (.daily %>% filter({{ listing_type }} == entire_home) %>% nrow() == 0) {
      warning(paste0("The supplied argument to `entire_home` returns no ",
                     "matches in the input table. Are you sure the argument ",
                     "is correct?"))
    }
  }


  ## Wrangle dates

  if (missing(start_date)) {
    start_date <-
      .daily %>%
      pull({{ date }}) %>%
      min(na.rm = TRUE)
  } else {
    start_date <- tryCatch(as.Date(start_date), error = function(e) {
      stop(paste0('The value of `start_date`` ("', start_date,
                  '") is not coercible to a date.'))
    })}

  if (missing(end_date)) {
    end_date <-
      .daily %>%
      pull({{ date }}) %>%
      max(na.rm = TRUE)
  } else {
    end_date <- tryCatch(as.Date(end_date), error = function(e) {
      stop(paste0('The value of `end_date` ("', end_date,
                  '") is not coercible to a date.'))
    })}


  ## Filter daily file

  if (lt_flag) .daily <- .daily %>% filter({{ listing_type }} == entire_home)

  .daily <-
    .daily %>%
    filter({{ status }} %in% c("A", "R"), {{ date }} >= start_date - 364,
           {{ date }} <= end_date)

  ## Rename fields to make data.table functions work

  .daily <-
    .daily %>%
    rename(property_ID = {{ property_ID }},
           date = {{ date }},
           status = {{ status }},
           listing_type = {{ listing_type }})


  ## Perform calculations

  setDT(.daily)

  if (!quiet) message("Identifying FREH listings. (",
                      substr(Sys.time(), 12, 19), ")")

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


  ## Rename fields to match input fields

  .daily <-
    .daily %>%
    rename({{ property_ID }} := .data$property_ID,
           {{ date }} := .data$date)


  ## Return output

  total_time <- Sys.time() - time_1

  if (!quiet) {message("Analysis complete. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (!quiet) {message("Total time: ",
                       substr(total_time, 1, 5), " ",
                       attr(total_time, "units"), ".")}

  return(.daily)

}
