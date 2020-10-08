#' Function to process raw property STR tables into UPGo format
#'
#' \code{strr_process_property} takes raw property tables from AirDNA and cleans
#' them for analysis or for upload in the UPGo database storage format.
#'
#' A function for cleaning raw property tables from AirDNA and preparing them
#' for subsequent analysis or upload in the UPGo format. The function also
#' produces error files which identify possible corrupt or missing lines in the
#' input file.
#'
#' The function expects the input property file to have either 56 fields (the
#' default for a raw table from AirDNA) or 36 fields (the default for UPGo,
#' after the `Zipcode`, `Average Daily Rate (USD)`,
#' `Average Daily Rate (Native)`, `Annual Revenue LTM (USD)`,
#' `Annual Revenue LTM (Native)`, `Occupancy Rate LTM`,
#' `Number of Bookings LTM`, `Count Reservation Days LTM`,
#' `Count Available Days LTM`, `Count Blocked Days LTM`,
#' `Calendar Last Updated`, `Security Deposit (Native)`,
#' `Cleaning Fee (Native)`, `Extra People Fee (Native)`,
#' `Published Nightly Rate (USD)`, `Published Monthly Rate (USD)`,
#' `Published Weekly Rate (USD)`, `Airbnb Listing URL`, and
#' `HomeAway Listing URL` fields are removed on import). Eventually the function
#' will be updated to support inputs from Inside Airbnb as well.
#'
#' Because the expectation is that the input files will be very large, the
#' function uses updating by reference on the property input table. This saves a
#' considerable amount of memory by avoiding making an unnecessary copy of the
#' input table, but has the side effect of changing the initial input file even
#' if the output is being assigned to a new object. An `update_by_reference`
#' argument may be added to the function in the future to override this
#' behaviour.
#'
#' @param property An unprocessed property data frame in the raw AirDNA format,
#' with either 37 or 56 fields.
#' @param keep_cols A logical scalar. If the `property` table has 56 fields,
#' should the superfluous 19 fields be kept, or should the table be trimmed to
#' the 37 fields which UPGo uses (default)?
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A list with three elements: 1) the processed `property` table; 2) an
#' `error` table identifying corrupt or otherwise invalid row entries; 3) a
#' `missing_geography` table identifying property_IDs with missing
#' latitude/longitude coordinates.
#' @export

strr_process_property <- function(property, keep_cols = FALSE, quiet = FALSE) {

  start_time <- Sys.time()


  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  ## Input checking ------------------------------------------------------------

  helper_check_property()
  helper_check_quiet()

  stopifnot(
    "`property` must have 37 or 56 fields." = length(property) %in% c(37, 56),
    "`keep_cols` must be a logical value." = is.logical(keep_cols))


  ## Initialize data.table -----------------------------------------------------

  ab_host <- country <- ha_host <- host_ID <- property_ID <- region <-
    ab_property <- ha_property <- NULL

  data.table::setDT(property)


  ### TRIM AND RENAME FIELDS ###################################################

  if (length(property) == 56) {
    data.table::setnames(property, c(
      "property_ID", "listing_title", "property_type", "listing_type",
      "created", "scraped", "country", "latitude", "longitude", "region",
      "city", "zipcode", "neighbourhood", "metro_area", "currency",
      "daily_rate", "daily_rate_native", "LTM_revenue", "LTM_revenue_native",
      "LTM_occupancy", "LTM_bookings", "LTM_reserved_days", "LTM_available",
      "LTM_blocked", "bedrooms", "bathrooms", "max_guests", "calendar_updated",
      "response_rate", "superhost", "premier_partner", "cancellation",
      "security_deposit", "security_deposit_native", "cleaning_fee",
      "cleaning_fee_native", "extra_people_fee", "extra_people_fee_native",
      "nightly_rate", "monthly_rate", "weekly_rate", "check_in_time",
      "check_out_time", "minimum_stay", "num_reviews", "num_photos",
      "instant_book", "rating", "ab_property", "ab_host", "ab_listing_url",
      "ab_image_url", "ha_property", "ha_host", "ha_listing_url",
      "ha_image_url"))

    if (!keep_cols) {
      helper_message("Dropping extra fields.")

      property[, c("zipcode", "daily_rate", "daily_rate_native", "LTM_revenue",
                   "LTM_revenue_native", "LTM_occupancy", "LTM_bookings",
                   "LTM_reserved_days", "LTM_available", "LTM_blocked",
                   "calendar_updated", "security_deposit_native",
                   "cleaning_fee_native", "extra_people_fee_native",
                   "nightly_rate", "monthly_rate", "weekly_rate",
                   "ab_listing_url", "ha_listing_url") := NULL]
      }
    } else {
      data.table::setnames(property, c(
        "property_ID", "listing_title", "property_type", "listing_type",
        "created", "scraped", "country", "latitude", "longitude", "region",
        "city", "neighbourhood", "metro_area", "currency", "bedrooms",
        "bathrooms", "max_guests", "response_rate", "superhost",
        "premier_partner", "cancellation", "security_deposit", "cleaning_fee",
        "extra_people_fee", "check_in_time", "check_out_time", "minimum_stay",
        "num_reviews", "num_photos", "instant_book", "rating", "ab_property",
        "ab_host", "ab_image_url", "ha_property", "ha_host", "ha_image_url"))
    }


  ### PRODUCE ERROR FILES ######################################################

  ## Produce `error` -----------------------------------------------------------

  helper_message(
    "(1/5) Identifying entries with invalid property_ID or listing_type.",
    .type = "open")

  error <- property[0,]

  if (!is.null(attr(property, "problems"))) {
    error <- attr(property, "problems")
    error <- (dplyr::filter(error, .data$expected != "56 columns"))$row
    error <- property[error,]
  }

  error <- dplyr::bind_rows(error, dplyr::filter(
    property, is.na(.data$property_ID) | is.na(.data$listing_type)))

  error <- dplyr::bind_rows(error, dplyr::filter(
    property, !substr(.data$property_ID, 1, 3) %in% c("ab-", "ha-")))

  property <- dplyr::filter(property, !property_ID %in% error$property_ID)

  helper_message(
    "(1/5) Entries with invalid property_ID or listing_type identified.",
    .type = "close")

  ## Produce `missing_geography` -----------------------------------------------

  helper_message("(2/5) Identifying entries with missing geography.",
                 .type = "open")

  missing_geography <-
    dplyr::filter(property, is.na(.data$latitude) | is.na(.data$longitude))

  property <-
    dplyr::filter(property, !is.na(.data$latitude), !is.na(.data$longitude))

  helper_message("(2/5) Entries with missing geography identified.",
                 .type = "close")


  ### REPLACE PROBLEMATIC CHARACTERS ###########################################

  helper_message("(3/5) Removing problematic characters.", .type = "open")

  property$listing_title <- gsub("\n|\r|\"|\'", "", property$listing_title)

  helper_message("(3/5) Problematic characters removed.", .type = "close")


  ### ADD AND MUTATE FIELDS ####################################################

  ## Add host_ID field ---------------------------------------------------------

  helper_message("(4/5) Adding new host_ID and housing fields.", .type = "open")

  data.table::setDT(property)[, host_ID := as.character(ab_host)
                              ][is.na(ab_host), host_ID := ha_host]


  ## Add housing field ---------------------------------------------------------

  property <- strr_housing(property)

  property <-
    dplyr::relocate(property, .data$host_ID, .after = .data$property_ID)

  property <-
    dplyr::relocate(property, .data$housing, .after = .data$scraped)

  property <-
    dplyr::relocate(property, .data$country, .after = .data$longitude)

  helper_message("(4/5) New host_ID and housing fields added.", .type = "close")


  ## Rename country names to match actual Airbnb usage -------------------------

  helper_message("(5/5) Harmonizing country names with Airbnb's usage.",
                 .type = "open")

  data.table::setDT(
    property
    )[country == "Bosnia-Herzegovina",
      country := "Bosnia and Herzegovina"
      ][country == "Brunei Darussalam", country := "Brunei"
        ][country == "C\u00f4te d'Ivoire - Republic of",
          country := "Ivory Coast"
          ][country == "Democratic Republic of Congo",
            country := "Democratic Republic of the Congo"
            ][country == "Curacao", country := "Cura\u00e7ao"
              ][country == "Falkland Islands",
                country := "Falkland Islands (Malvinas)"
                ][country == "Lao", country := "Laos"
                  ][country == "Macedonia - Republic of", country := "Macedonia"
                    ][country == "Palestine",
                      country := "Palestinian Territories"
                      ][country == "Republic of Congo", country := "Congo"
                        ][country == "Saint-Martin", country := "Saint Martin"
                          ][region == "Bhutan", country := "Bhutan"
                            ][region == "Puerto Rico", country := "Puerto Rico"
                              ][region == "United States Virgin Islands",
                                country := "U.S. Virgin Islands"]

  helper_message("(5/5) Country names harmonized with Airbnb's usage.",
                 .type = "close")

  ## Add prefixes to ab_property and ha_property fields ------------------------

  property[, c("ab_property", "ha_property") := list(
    ifelse(is.na(ab_property), NA_character_, paste0("ab-", ab_property)),
    ifelse(is.na(ha_property), NA_character_, paste0("ha-", ha_property)))]


  ### RETURN OUTPUT ############################################################

  helper_message("Processing complete.", .type = "final")

  return(list(
    dplyr::as_tibble(property),
    dplyr::as_tibble(error),
    dplyr::as_tibble(missing_geography)
    ))

}
