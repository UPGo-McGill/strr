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
#' function uses assignment by reference on the daily input table to change it
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
#' @importFrom data.table setDT setnames
#' @importFrom dplyr %>% anti_join bind_rows distinct filter inner_join
#' @importFrom dplyr select semi_join
#' @importFrom rlang .data set_names
#' @importFrom tibble as_tibble
#' @export

strr_process_daily <- function(daily, property, keep_cols = FALSE,
                               quiet = FALSE) {

  time_1 <- Sys.time()


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


  ### Join property file to daily file, and begin error table ##################

  helper_progress_message("Beginning error check.")

  error <-
    daily %>%
    anti_join(property, by = "property_ID")

  helper_progress_message("Rows missing from property file identified.")

  prop_cols <-
    c("property_ID", "host_ID", "listing_type", "created", "scraped", "housing",
      "country", "region", "city")

  daily <-
    daily %>%
    inner_join(select(property, prop_cols), by = "property_ID")

  helper_progress_message("Listing data joined into daily file.")


  ### Process date, status and duplicates ######################################

  ## Find rows with missing or invalid date or status

  # This combination of filters is the fastest and least memory-intensive
  error_date <- stats::na.omit(daily, cols = c("date"), invert = TRUE)
  error_status <- filter(daily, !status %in% c("A", "U", "B", "R"))

  new_error <- rbindlist(list(error_date, error_status))

  if (nrow(new_error) > 0) {
    daily <-
      daily %>%
      # Do join by all fields
      anti_join(new_error, by = names(daily))

    # Only take the first six rows of new_error to match length of error
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
    "Rows with missing or invalid date or status identified.")


  ### Remove duplicate entries by price, but don't add to error file ###########

  setDT(daily)

  # Prepare to calculate number of duplicate rows
  dup_rows <- nrow(daily)

  # This query is faster in data.table than dplyr
  daily <- daily[!is.na(price),]

  # Save number of duplicate rows for subsequent error checking
  dup_rows <- dup_rows - nrow(daily)

  # Add as attribute
  attr(error, "duplicate_rows") <- attr(error, "duplicate_rows") + dup_rows

  helper_progress_message("Duplicate rows removed.")


  ### Produce missing_rows table ###############################################

  # Faster and less memory-intensive to split up
  missing_rows <-
    daily[, .(count = .N, low = min(date), high = max(date)), by = "property_ID"
          ][, c("full_count", "dif") := list(as.integer(high - low) + 1,
                                             as.integer(high - low) + 1 - count)
            ][, c("low", "high") := NULL
              ][dif > 0]

  helper_progress_message("Missing rows identified.")


  ### Produce daily and daily_inactive tables ##################################

  daily_inactive <-
    daily[date < created | date > scraped, verbose = TRUE
          ][order(property_ID, date)
            ][, c("created", "scraped") := NULL]

  daily <-
    daily[date >= created & date <= scraped
          ][order(property_ID, date)
            ][, c("created", "scraped") := NULL]

  helper_progress_message("Rows outside active listing period identified.")


  ### Return output ############################################################

  ## Set classes of outputs

  daily <- as_tibble(daily)
  class(daily) <- append(class(daily), "strr_daily")

  daily_inactive <- as_tibble(daily_inactive)
  class(daily_inactive) <- append(class(daily_inactive), "strr_daily")

  error <- as_tibble(error)
  missing_rows <- as_tibble(missing_rows)


  ## Return output

  helper_progress_message("Processing complete.", .final = TRUE)

  return(list(daily, daily_inactive, error, missing_rows))
}
