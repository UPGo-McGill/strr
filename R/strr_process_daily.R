#' Function to process raw daily STR tables into UPGo format
#'
#' \code{strr_process_daily} takes raw daily tables from AirDNA and cleans them
#' to prepare for compression into the UPGo database storage format
#' (using \code{\link{strr_compress}}).
#'
#' A function for cleaning raw daily activity tables from AirDNA and preparing
#' them for compression into the UPGo format. The function also produces error
#' files which identify possible corrupt or missing lines in the input file.
#'
#' The function expects the input daily file to have either ten fields (the
#' default for a raw table from AirDNA) or six fields (the default for UPGo,
#' after the "Price (Native)", "Currency Native", "Airbnb Property ID", and
#' "HomeAway Property ID" fields are removed on import).
#'
#' The function expects the input property file to be formatted in the UPGo
#' style, in particular with fields named "property_ID", "created", and
#' "scraped". Eventually function arguments may be supplied to allow these
#' field names to be overruled.
#'
#' Because the expectation is that the input files will be very large, the
#' function uses updating by reference on the daily input table to change it
#' to data.table class prior to processing. This saves a considerable amount of
#' memory by avoiding making an unnecessary copy of the input daily table, but
#' has the side effect of the initial input file being changed to a data.table.
#'
#' @param daily An unprocessed daily table in the raw AirDNA format, with either
#' ten or six fields.
#' @param property A property table processed in the UPGo style.
#' @param keep_cols A logical scalar. If the `daily` table has 10 fields,
#' should the superfluous 4 fields be kept, or should the table be trimmed to
#' the 6 fields which UPGo uses (default)?
#' @param old_format Temporary option to add created/scraped and not split
#' output into active and inactive.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A list with four elements: 1) the processed daily table, ready for
#' compression with \code{\link{strr_compress}}; 2) a processed daily_inactive
#' table, containing the rows which fall outside a listing's active period (as
#' determined by `created` and `scraped` fields in the property table), ready
#' for compression with \code{\link{strr_compress}}; 3) an error table
#' identifying corrupt or otherwise invalid row entries; 4) a missing_rows table
#' identifying property_IDs with missing dates in between their first and last
#' date entries, and therefore potentially missing data.
#' @importFrom data.table setDT setnames
#' @importFrom dplyr %>% anti_join distinct filter inner_join select
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_process_daily <- function(daily, property, keep_cols = FALSE,
                               old_format = FALSE, quiet = FALSE) {

  time_1 <- Sys.time()

  # Print \n on exit so error messages don't collide with progress messages
  on.exit(if (!quiet) message())

  helper_progress_message("Beginning processing.")


  ### Error checking and initialization ########################################

  .datatable.aware = TRUE

  count <- created <- dif <- full_count <- high <- low <-  price <-
    property_ID <- scraped <- status <- NULL

  ## Check that quiet is a logical

  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }

  ## Check that daily and property are present and are data frames

  if (missing(daily)) {
    stop("The argument `daily` is missing.")
  }

  if (missing(property)) {
    stop("The argument `property` is missing.")
  }

  if (!inherits(daily, "data.frame")) {
    stop("The object supplied to the `daily` argument must be a data frame.")
  }

  if (!inherits(property, "data.frame")) {
    stop("The object supplied to the `property` argument must be a data frame.")
  }


  ## Check number of fields and rename

  if (length(daily) == 6) {

    setnames(daily, c("property_ID", "date", "status", "booked_date", "price",
                      "res_ID"))

  } else if (length(daily) == 10) {

    setnames(daily, c("property_ID", "date", "status", "booked_date", "price",
                      "price_native", "currency", "res_ID", "ab_property",
                      "ha_property"))

    if (!keep_cols) {

      setDT(daily)

      daily[, c("price_native", "currency", "ab_property",
                "ha_property") := NULL]

      }
  } else stop("The `daily` table must have either six or ten fields.")


  ## Get number of rows for error checking

  daily_rows <- nrow(daily)


  ### Join property file to daily file, and begin error table ##################

  helper_progress_message("(1/6) Identifying rows missing from property file.",
                          .type = "open")

  error <-
    daily %>%
    anti_join(property, by = "property_ID")

  helper_progress_message("(1/6) Rows missing from property file identified.",
                          .type = "close")

  helper_progress_message("(2/6) Joining listing data into daily file.",
                          .type = "open")

  prop_cols <-
    c("property_ID", "host_ID", "listing_type", "created", "scraped", "housing",
      "country", "region", "city")

  daily <-
    daily %>%
    inner_join(select(property, prop_cols), by = "property_ID")

  helper_progress_message("(2/6) Listing data joined into daily file.",
                          .type = "close")


  ### Process date, status and duplicates ######################################

  ## Find rows with missing or invalid date or status

  helper_progress_message(
    "(3/6) Identifying rows with missing or invalid date or status.",
    .type = "open")

  setDT(daily)

  # This combination of filters is the fastest and least memory-intensive
  error_date <- stats::na.omit(daily, cols = c("date"), invert = TRUE)
  error_status <- filter(daily, !status %in% c("A", "U", "B", "R"))

  new_error <- distinct(rbindlist(list(error_date, error_status)))

  if (nrow(new_error) > 0) {
    daily <-
      daily %>%
      # Do join by all fields
      anti_join(new_error, by = names(daily))

    setDT(daily)

    # Only take the first six columns of new_error to match length of error
    error <- bind_rows(error, select(new_error, 1:6))
  }


  ## Discard duplicates

  # Prepare to calculate number of duplicate rows
  dup_rows <- nrow(error)

  # Discard duplicates
  error <- distinct(error)

  # Save number of duplicate rows for subsequent error checking
  dup_rows <- dup_rows - nrow(error)

  # Add as attribute
  attr(error, "duplicate_rows") <- dup_rows

  helper_progress_message(
    "(3/6) Rows with missing or invalid date or status identified.",
    .type = "close")


  ### Remove duplicate entries by price, but don't add to error file ###########

  helper_progress_message("(4/6) Removing duplicate rows.", .type = "open")

  # Prepare to calculate number of duplicate rows
  dup_rows <- nrow(daily)

  # This query is faster in data.table than dplyr
  daily <- daily[!is.na(price),]

  # Save number of duplicate rows for subsequent error checking
  dup_rows <- dup_rows - nrow(daily)

  # Add as attribute
  attr(error, "duplicate_rows") <- attr(error, "duplicate_rows") + dup_rows

  helper_progress_message("(4/6) Duplicate rows removed.", .type = "close")


  ### Produce missing_rows table ###############################################

  helper_progress_message("(5/6) Identifying missing rows.", .type = "open")

  # Faster and less memory-intensive to split up
  missing_rows <-
    daily[, .(count = .N, low = min(date), high = max(date)), by = "property_ID"
          ][, c("full_count", "dif") := list(as.integer(high - low) + 1,
                                             as.integer(high - low) + 1 - count)
            ][, c("low", "high") := NULL
              ][dif > 0]

  helper_progress_message("(5/6) Missing rows identified.", .type = "close")


  ### Produce daily and daily_inactive tables ##################################

  if (!old_format) { ## TKTK Delete after new database is deployed

    helper_progress_message(
      "(6/6) Identifying rows outside active listing period.", .type = "open")

  daily_inactive <-
    daily[date < created | date > scraped, verbose = TRUE
          ][order(property_ID, date)
            ][, c("created", "scraped") := NULL]

  daily <-
    daily[date >= created & date <= scraped
          ][order(property_ID, date)
            ][, c("created", "scraped") := NULL]

  helper_progress_message(
    "(6/6) Rows outside active listing period identified.", .type = "close")

  } ## TKTK Delete after new database is deployed

  ### Return output ############################################################

  ## Set classes of outputs

  daily <- as_tibble(daily)
  class(daily) <- append(class(daily), "strr_daily")

  if (!old_format) { ## TKTK Delete after new database is deployed

  daily_inactive <- as_tibble(daily_inactive)
  class(daily_inactive) <- append(class(daily_inactive), "strr_daily")

  } ## TKTK Delete after new database is deployed

  error <- as_tibble(error)
  missing_rows <- as_tibble(missing_rows)


  ## Compare length of input with lengths of outputs

  if (!old_format) { ## TKTK Delete after new database is deployed

  if (daily_rows != nrow(daily) + nrow(daily_inactive) + nrow(error) +
      attr(error, "duplicate_rows")) {
    warning(
      glue::glue("The number of rows of the input daily table ({daily_rows}) "),
      glue::glue("does not equal the combined rows of the output tables "),
      glue::glue("({nrow(daily) + nrow(daily_inactive)}) plus the number of "),
      glue::glue("duplicated rows ({attr(error, 'duplicate_rows')})."))
  }
  } ## TKTK Delete after new database is deployed


  ## Return output

  helper_progress_message("Processing complete.", .type = "final")

  if (!old_format) { ## TKTK Delete after new database is deployed

  return(list(daily, daily_inactive, error, missing_rows))

  } else {return(list(daily, error, missing_rows))} ## TKTK Delete

}
