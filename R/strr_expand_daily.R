#' Function to expand compressed STR daily tables
#'
#' \code{strr_expand_daily} takes a compressed STR daily file and expands it to
#' a one-row-per-date format.
#'
#' A function for probablistically assigning STR listings to administrative
#' geographies (e.g. census tracts) based on reported latitude/longitude.
#' The function works by combining a known probability density function (e.g.
#' Airbnb's spatial obfuscation of listing locations) with an additional source
#' of information about possible listing locations--either population or housing
#' densities.
#'
#' @param daily An sf or sp point-geometry object, in a projected coordinate
#'   system.
#' @param cores A positive integer scalar. How many processing cores should be
#'   used to perform the computationally intensive numeric integration step?
#' @return The output will be the input points object with a new `winner` field
#'   appended. The `winner` field specifies which polygon from the polys object
#'   was probabilistically assigned to the listing, using the field identified
#'   in the `poly_ID` argument. If diagnostic == TRUE, a `candidates` field will
#'   also be appended, which lists the possible polygons for each point, along
#'   with their probabilities.
#' @importFrom dplyr %>% as_tibble enquo filter group_by left_join mutate
#' @importFrom dplyr select summarize
#' @importFrom methods is
#' @importFrom rlang := .data
#' @importFrom sf st_area st_as_sf st_buffer st_coordinates st_crs
#' @importFrom sf st_drop_geometry st_intersection st_set_agr st_sfc
#' @importFrom sf st_transform
#' @importFrom stats dnorm
#' @export

strr_expand_daily <- function(daily, start = NULL, end = NULL, cores = 1) {

  ## ERROR CHECKING AND ARGUMENT INITIALIZATION

  # Check that cores is an integer > 0

  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }

  # Check that dates are coercible to date class, then coerce them

  if (!missing(start)) {
    start <- tryCatch(as.Date(start), error = function(e) {
      stop(paste0('The value of `start`` ("', start,
                  '") is not coercible to a date.'))
    })}

  if (!missing(end)) {
    end <- tryCatch(as.Date(end), error = function(e) {
      stop(paste0('The value of `end` ("', end,
                  '") is not coercible to a date.'))
    })}


  ## PREPARE DATE FIELD

  daily <-
    daily %>%
    mutate(date = map2(start_date, end_date, ~{.x:.y}))


  ## SINGLE-CORE VERSION

  if (cores == 1) {

    daily <-
      daily %>%
      unnest(cols = c(date)) %>%
      mutate(date = as.Date(date, origin = "1970-01-01")) %>%
      select(property_ID, date, everything(), -start_date, -end_date)

  ## MULTI-CORE VERSION

  } else {

    daily_list <-
      split(daily, 0:(nrow(daily) - 1) %/% ceiling(nrow(daily) / 100))

    daily <-
      pblapply(daily_list, function(x) {
        x %>%
          unnest(cols = c(date)) %>%
          mutate(date = as.Date(date, origin = "1970-01-01")) %>%
          select(property_ID, date, everything(), -start_date, -end_date)
      }, cl = cores) %>%
      bind_rows()
  }


  ## OPTIONALLY TRIM BASED ON START/END DATE

  if (!missing(start)) {
    daily <- filter(daily, date >= start)
  }

  if (!missing(end)) {
    daily <- filter(daily, date <= end)
  }


  ## OUTPUT DATA FRAME

  return(daily)
}
