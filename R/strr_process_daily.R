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
#' @export

strr_process_daily <- function(daily, property, keep_cols = FALSE,
                               quiet = FALSE) {

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  helper_check_daily()
  helper_check_property()
  helper_check_quiet()

  helper_message("Beginning processing.")


  ## Set up data.table variables -----------------------------------------------

  count <- created <- dif <- full_count <- high <- low <-  price <-
    property_ID <- scraped <- status <- NULL


  ## Check number of fields and rename -----------------------------------------

  if (length(daily) == 6) {

    data.table::setnames(daily, c("property_ID", "date", "status",
                                  "booked_date", "price", "res_ID"))

  } else if (length(daily) == 10) {

    data.table::setnames(daily, c("property_ID", "date", "status",
                                  "booked_date", "price", "price_native",
                                  "currency", "res_ID", "ab_property",
                                  "ha_property"))

    if (!keep_cols) {

      data.table::setDT(daily)

      daily[, c("price_native", "currency", "ab_property",
                "ha_property") := NULL]

      }
  } else stop("The `daily` table must have either six or ten fields.")


  ## Get number of rows for error checking -------------------------------------

  daily_rows <- nrow(daily)


  ### JOIN PROPERTY FILE TO DAILY FILE, AND BEING ERROR TABLE ##################

  helper_message("(1/6) Identifying rows missing from property file.",
                 .type = "open")

  error <- dplyr::anti_join(daily, property, by = "property_ID")

  helper_message("(1/6) Rows missing from property file identified.",
                 .type = "close")

  helper_message("(2/6) Joining listing data into daily file.", .type = "open")

  prop_cols <- c("property_ID", "host_ID", "listing_type", "created", "scraped",
                 "housing", "country", "region", "city")

  daily <- dplyr::inner_join(daily, dplyr::select(property, prop_cols),
                             by = "property_ID")

  helper_message("(2/6) Listing data joined into daily file.", .type = "close")


  ### PROCESS DATE, STATUS AND DUPLICATES ######################################

  ## Find rows with missing or invalid date or status --------------------------

  helper_message(
    "(3/6) Identifying rows with missing or invalid date or status.",
    .type = "open")

  data.table::setDT(daily)

  # This combination of filters is the fastest and least memory-intensive
  error_date <- stats::na.omit(daily, cols = c("date"), invert = TRUE)
  error_status <- dplyr::filter(daily, !.data$status %in% c("A", "U", "B", "R"))

  new_error <- dplyr::distinct(
    data.table::rbindlist(list(error_date, error_status)))

  if (nrow(new_error) > 0) {
    # Do join by all fields
    daily <- dplyr::anti_join(daily, new_error, by = names(daily))

    data.table::setDT(daily)

    # Only take the first six columns of new_error to match length of error
    error <- dplyr::bind_rows(error, dplyr::select(new_error, 1:6))
  }


  ## Discard duplicates --------------------------------------------------------

  # Prepare to calculate number of duplicate rows
  dup_rows <- nrow(error)

  # Discard duplicates
  error <- dplyr::distinct(error)

  # Save number of duplicate rows for subsequent error checking
  dup_rows <- dup_rows - nrow(error)

  # Add as attribute
  attr(error, "duplicate_rows") <- dup_rows

  helper_message(
    "(3/6) Rows with missing or invalid date or status identified.",
    .type = "close")


  ### REMOVE DUPLICATE ENTRIES BY PRICE, BUT DON'T ADD TO ERROR FILE ###########

  helper_message("(4/6) Removing duplicate rows.", .type = "open")

  # Prepare to calculate number of duplicate rows
  dup_rows <- nrow(daily)

  # This query is faster in data.table than dplyr
  daily <- daily[!is.na(price)]

  # Save number of duplicate rows for subsequent error checking
  dup_rows <- dup_rows - nrow(daily)

  # Add as attribute
  attr(error, "duplicate_rows") <- attr(error, "duplicate_rows") + dup_rows

  helper_message("(4/6) Duplicate rows removed.", .type = "close")


  ### PRODUCE missing_rows TABLE ###############################################

  helper_message("(5/6) Identifying missing rows.", .type = "open")

  # Faster and less memory-intensive to split up
  missing_rows <-
    daily[, .(count = .N, low = min(date), high = max(date)), by = "property_ID"
          ][, c("full_count", "dif") := list(as.integer(high - low) + 1,
                                             as.integer(high - low) + 1 - count)
            ][, c("low", "high") := NULL
              ][dif > 0]

  helper_message("(5/6) Missing rows identified.", .type = "close")


  ### PRODUCE daily AND daily_inactive TABLES ##################################

  helper_message("(6/6) Identifying inactive rows.", .type = "open")

  daily_inactive <-
    daily[date < created | date > scraped | status == "U"
          ][order(property_ID, date)
            ][, c("created", "scraped") := NULL]

  daily <-
    daily[date >= created & date <= scraped & status != "U"
          ][order(property_ID, date)
            ][, c("created", "scraped") := NULL]

  helper_message("(6/6) Inactive rows identified.", .type = "close")


  ### RETURN OUTPUT ############################################################

  daily <- dplyr::as_tibble(daily)
  daily_inactive <- dplyr::as_tibble(daily_inactive)
  error <- dplyr::as_tibble(error)
  missing_rows <- dplyr::as_tibble(missing_rows)


  ## Compare length of input with lengths of outputs ---------------------------

  if (daily_rows != nrow(daily) + nrow(daily_inactive) + nrow(error) +
      attr(error, "duplicate_rows")) {
    warning("The number of rows of the input daily table (", daily_rows,
            ") does not equal the combined rows of the output tables (",
            nrow(daily) + nrow(daily_inactive),
            ") plus the number of duplicated rows (",
            attr(error, 'duplicate_rows'), ").")
  }


  ## Return output -------------------------------------------------------------

  helper_message("Processing complete.", .type = "final")

  return(list(daily, daily_inactive, error, missing_rows))

}
