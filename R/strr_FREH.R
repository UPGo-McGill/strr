#' Identify frequently rented entire-home (FREH) listings
#'
#' \code{strr_FREH} takes a table of daily STR activity and identifies listings
#' which met a given standard of availability and activity over a specified
#' time period.
#'
#' \code{strr_FREH} is named after "frequently rented entire-home" (FREH)
#' listings, which it identifies by examining rolling windows of activity to
#' find listings with more than a specified number of reserved or available
#' nights in a specified time period. By default, the function identifies
#' entire-home listings which are available a majority of the year and reserved
#' at least 90 nights a year, and reports for each date within the requested
#' time range #' whether each entire-home listing satisfies or fails to satisfy
#' these criteria.
#'
#' While the inspiration for the function is identifying FREH listings, all the
#' function's parameters can be modified; so, for example, it can instead
#' identify all listing types reserved at least once in a month.
#'
#' @param daily A data frame of daily STR activity in standard UPGo format.
#' @param start_date A character string of format YYYY-MM-DD indicating the
#'   first date for which to return output. If NULL (default), all dates will
#'   be used.
#' @param end_date A character string of format YYYY-MM-DD indicating the last
#'   date for which to run the analysis.
#' @param property_ID The name of a character or numeric variable in the `daily`
#'   table which uniquely identifies STR listings.
#' @param date The name of a date variable in the `daily` table.
#' @param status The name of a character variable in the `daily` table which
#' identifies the activity status of a listing on a give date.
#' @param status_types A two-length character vector which identifies the
#' values in the `status` variable indicating "reserved" and "available" status
#' respectively. The default value is \code{c("R", "A")}.
#' @param listing_type The name of a character variable in the `daily`
#'   table which identifies entire-home listings. Set this argument to FALSE
#'   to use all listings in the `daily` table.
#' @param entire_home A character string which identifies the value of the
#'   `listing_type` variable to be used to find entire-home listings. This field
#'   is ignored if `listing_type` is FALSE.
#' @param n_days An integer scalar which determines how many days should be used
#' to evaluate each listing's activity status. The default is 365 days (one
#' year).
#' @param r_cut An integer scalar. The threshold for number of reserved days in
#' the last `n_days` which qualifies for "frequently rented".
#' @param ar_cut An integer scalar. The threshold for number of available or
#' reserved days in the last `n_days` which qualifies for "frequently rented".
#' @param quiet A logical vector. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be a tidy data frame of identified FREH listings,
#'   organized with the following fields: `property_ID` (or whatever name was
#'   passed to the property_ID argument): A character vector with the ID code of
#'   the listings. `date`: The date for which the FREH status is being reported.
#'   `FREH`: A logical scalar indicating whether, on a given date, the given
#'   listing exceeded the `r_cut` and `ar_cut` thresholds over the number of
#'   days specified by `n_days`.
#' @export

strr_FREH <- function(daily, start_date, end_date, property_ID = property_ID,
                      date = date, status = status, status_types = c("R", "A"),
                      listing_type = listing_type,
                      entire_home = "Entire home/apt", n_days = 365, r_cut = 90,
                      ar_cut = 183, quiet = FALSE) {


  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  helper_check_daily(rlang::ensyms(property_ID, date, status))

  # start_date and end_date checked below during type conversion

  # Check that status_types arguments are plausible
  if (length(status_types) != 2) {
    stop("The `status_type` argument must be a vector of length 2.")
  }

  if (nrow(dplyr::filter(daily, {{ status }} == status_types[1])) == 0) {
    warning(paste0("The first supplied argument to `status_types` returns no ",
                   "matches in the input table. Are you sure the argument ",
                   "is correct?"))
  }

  if (nrow(dplyr::filter(daily, {{ status }} == status_types[2])) == 0) {
    warning(paste0("The second supplied argument to `status_types` returns no ",
                   "matches in the input table. Are you sure the argument ",
                   "is correct?"))
  }

  # Set lt_flag and check validity of listing_type
  lt_flag <-
    tryCatch({
      # If listing_type is a field in points, set lt_flag = TRUE
      dplyr::pull(daily, {{ listing_type }})
      TRUE},
      error = function(e) {tryCatch({
        # If listing_type == FALSE, set lt_flag = FALSE
        if (!listing_type) { FALSE
          } else stop("`listing_type` must be a valid field name or FALSE.")
        },
        error = function(e2) {
          # Otherwise, fail with an informative error
          stop("`listing_type` must be a valid field name or FALSE.")
          }
        )})

  # Check entire_home argument
  if (lt_flag) {
    if (nrow(dplyr::filter(daily, {{ listing_type }} == entire_home)) == 0) {
      warning(paste0("The supplied argument to `entire_home` returns no ",
                     "matches in the input table. Are you sure the argument ",
                     "is correct?"))
    }
  }

  # Check that n_days is an integer > 0
  n_days <- floor(n_days)
  if (n_days <= 0) {
    stop("The argument `n_days` must be a positive integer.")
  }

  # Check that r_cut is an integer > 0 and <= n_days
  r_cut <- floor(r_cut)
  if (r_cut <= 0 | r_cut > n_days) {
    stop("The argument `r_cut` must be a positive integer <= `n_days`.")
  }

  # Check that ar_cut is an integer > 0, <= n_days and >= r_cut
  ar_cut <- floor(ar_cut)
  if (ar_cut <= 0 | ar_cut > n_days | ar_cut < r_cut) {
    stop("The argument `ar_cut` must be a positive integer <= `n_days` and ",
         ">= than `r_cut`.")
  }

  helper_check_quiet()


  ## Prepare data.table and future variables -----------------------------------

  .datatable.aware = TRUE

  # Silence R CMD check for data.table fields
  R <- AR <- NULL

  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    # Replace map_* with future_map_*
    map <- furrr::future_map
    map_dfr <-
      function(.x, .f) furrr::future_map_dfr(
        .x, .f, .options = furrr::future_options(
          globals = c("start_date", "end_date", "daily", "ar_cut", "r_cut")))

    # Remove limit on globals size
    options(future.globals.maxSize = +Inf)

    # Make sure data.table is single-threaded within the helper
    threads <- data.table::setDTthreads(1)

    # Set up on.exit expression for errors
    on.exit({
      # Flush out any stray multicore processes
      map(1:future::nbrOfWorkers(), ~.x)

      # Restore future global export limit
      .Options$future.globals.maxSize <- NULL

      # Restore data.table threads
      data.table::setDTthreads(threads)

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


  ### PREPARE TABLE FOR ANALYSIS ###############################################

  ## Drop geometry if table is sf ----------------------------------------------

  if (inherits(daily, "sf")) {
    daily <- sf::st_drop_geometry(daily)
  }


  ## Wrangle dates -------------------------------------------------------------

  if (missing(start_date)) {
    start_date <- min(dplyr::pull(daily, {{ date }}), na.rm = TRUE)
  } else {
    start_date <- tryCatch(as.Date(start_date), error = function(e) {
      stop(paste0('The value of `start_date`` ("', start_date,
                  '") is not coercible to a date.'))
    })}

  if (missing(end_date)) {
    end_date <- max(dplyr::pull(daily, {{ date }}), na.rm = TRUE)
  } else {
    end_date <- tryCatch(as.Date(end_date), error = function(e) {
      stop(paste0('The value of `end_date` ("', end_date,
                  '") is not coercible to a date.'))
    })}


  ## Rename fields to make data.table functions work ---------------------------

  daily <- dplyr::rename(daily,
                         property_ID = {{ property_ID }},
                         date = {{ date }},
                         status = {{ status }})


  ## Filter daily file and select necessary columns ----------------------------

  data.table::setDT(daily)

  if (lt_flag) {

    daily <- dplyr::rename(daily, listing_type = {{ listing_type }})
    daily <- daily[listing_type == entire_home]

  }

  daily <-
    daily[status %in% c("A", "R") & date >= start_date - 364 & date <= end_date]

  # Only select needed fields, to reduce object size for remote transfer
  daily[, setdiff(names(daily), c("property_ID", "date", "status")) := NULL]


  ### PERFORM CALCULATIONS #####################################################

  helper_message("(1/2) Analyzing data, using {helper_plan()}.")


  ## Function to be iterated over ----------------------------------------------

  date_fun <- function(.x) {
    daily <- daily[date >= .x - 364 & date <= .x]
    daily[, AR := .N, by = "property_ID"]
    daily[, R := sum(status == "R"), by = "property_ID"]
    daily[, list(date = as.Date(.x, origin = "1970-01-01"),
                 FREH = as.logical((mean(AR) >= ar_cut) *
                                     (mean(R) >= r_cut))),
          by = "property_ID"]
  }


  ## Run function --------------------------------------------------------------

  if (progress) {

    handler_strr("Analyzing date")

    progressr::with_progress({

      # Initialize progress bar
      .strr_env$pb <- progressr::progressor(along = start_date:end_date)

      daily <-
        map_dfr(start_date:end_date, ~{
          .strr_env$pb()
          date_fun(.x)})
    })

  } else daily <- map_dfr(start_date:end_date, date_fun)


  ### ARRANGE TABLE THEN RENAME FIELDS TO MATCH INPUT FIELDS ###################

  helper_message("(2/2) Arranging output.", .type = "open")

  data.table::setorderv(daily, c("property_ID", "date"))

  daily <- dplyr::rename(daily, {{ property_ID }} := .data$property_ID,
                         {{ date }} := .data$date)
  daily <- dplyr::as_tibble(daily)

  helper_message("(2/2) Output arranged.", .type = "close")


  ### RETURN OUTPUT ############################################################

  helper_message("Analysis complete.", .type = "final")

  if (requireNamespace("future", quietly = TRUE) &&
      requireNamespace("furrr", quietly = TRUE)) {

    # Set up on.exit expression for errors
    on.exit({

      # Restore future global export limit
      .Options$future.globals.maxSize <- NULL

      # Restore data.table threads
      data.table::setDTthreads(threads)

    })
  }

  return(daily)

}
