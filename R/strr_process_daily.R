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
#' @importFrom dplyr %>% anti_join bind_rows distinct filter inner_join mutate
#' @importFrom dplyr pull select semi_join
#' @importFrom rlang .data set_names
#' @importFrom tibble as_tibble
#' @export

strr_process_daily <- function(daily, property, keep_cols = FALSE,
                               quiet = FALSE) {

  time_1 <- Sys.time()


  ### Error checking and initialization ########################################

  .datatable.aware = TRUE

  count <- created <- dif <- full_count <- price <- property_ID <- scraped <-
    status <- NULL

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


  ### Produce error table ######################################################

  helper_progress_message("Beginning error check.", .quiet = quiet)


  ## Check for listings missing from property file

  error <-
    daily %>%
    anti_join(property, by = "property_ID")

  daily <-
    daily %>%
    semi_join(property, by = "property_ID")

  helper_progress_message("Rows missing from property file identified.")


  ## Find rows with missing or invalid date or status

  new_error <-
    daily %>%
    filter(is.na(date) | is.na(status), !status %in% c("A", "U", "B", "R"))

  if (length(new_error) > 0) {
    daily <-
      daily %>%
      anti_join(new_error, by = "property_ID")

    error <- rbindlist(list(error, new_error))
  }

  helper_progress_message(
    "Rows with missing or invalid date or status identified.")


  ## Remove duplicate listing entries by price, but don't add to error file

  setDT(daily)

  daily <-
    daily[!is.na(price),]

  helper_progress_message("Duplicate rows removed.")


  ### Produce missing_rows table ###############################################

  missing_rows <-
    daily[, .(count = .N,
              full_count = as.integer(max(date) - min(date) + 1),
              dif = as.integer(max(date) - min(date) + 1) - .N),
          by = property_ID
          ][dif != 0, ]

  helper_progress_message("Missing rows identified.")


  ### Produce daily and daily_inactive tables ##################################

  ## Join property file

  prop_cols <-
    c("property_ID", "host_ID", "listing_type", "created", "scraped", "housing",
      "country", "region", "city")

  daily <-
    daily %>%
    inner_join(select(property, prop_cols), by = "property_ID")

  helper_progress_message("Listing data joined into daily file.")


  ## Produce daily and daily_inactive

  setDT(daily)

  daily_inactive <- daily[date < created | date > scraped,]
  daily_inactive[, c("created", "scraped") := NULL]

  daily[, c("created", "scraped") := NULL]
  daily <- daily[!daily_inactive, on = c("property_ID", "date")]

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
