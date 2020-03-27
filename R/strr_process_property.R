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
#' default for a raw table from AirDNA) or 35 fields (the default for UPGo,
#' after the `Zipcode`, `Average Daily Rate (USD)`,
#' `Average Daily Rate (Native)`, `Annual Revenue LTM (USD)`,
#' `Annual Revenue LTM (Native)`, `Occupancy Rate LTM`,
#' `Number of Bookings LTM`, `Count Reservation Days LTM`,
#' `Count Available Days LTM`, `Count Blocked Days LTM`,
#' `Calendar Last Updated`, `Security Deposit (Native)`,
#' `Cleaning Fee (Native)`, `Extra People Fee (Native)`,
#' `Published Nightly Rate (USD)`, `Published Monthly Rate (USD)`,
#' `Published Weekly Rate (USD)`, `Airbnb Listing URL`,
#' `Airbnb Listing Main Image URL`, `HomeAway Listing URL`, and
#' `HomeAway Listing Main Image URL` fields are removed on import). Eventually
#' the function will be updated to support inputs from Inside Airbnb instead.
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
#' with either 35 or 56 fields.
#' @param keep_cols A logical scalar. If the `property` table has 56 fields,
#' should the superfluous 21 fields be kept, or should the table be trimmed to
#' the 35 fields which UPGo uses (default)?
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A list with three elements: 1) the processed property table, with 35
#' fields; 2) an error table identifying corrupt or otherwise invalid row
#' entries; 3) a missing_geography table identifying property_IDs with missing
#' latitude/longitude coordinates.
#' @importFrom data.table setDT setnames
#' @importFrom dplyr %>% as_tibble bind_rows case_when filter if_else mutate
#' @importFrom dplyr pull select
#' @importFrom rlang .data set_names
#' @importFrom stringr str_replace_all str_starts
#' @export

strr_process_property <- function(property, keep_cols = FALSE, quiet = FALSE) {

  time_1 <- Sys.time()


  ### Error checking and initialization ########################################

  .datatable.aware = TRUE

  ab_host <-country <- ha_host <- host_ID <- property_ID <- region <- NULL

  if (!inherits(property, "data.frame")) {
    stop("The input table must be a data frame.")
  }

  if (!length(property) %in% c(35, 56)) {
    stop("The input table must have either 35 or 56 fields.")
  }

  # Check that keep_cols and quiet are logical
  if (!is.logical(keep_cols)) {
    stop("The argument `keep_cols` must be a logical value (TRUE or FALSE).")
  }
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }

  setDT(property)


  ### Trim and rename fields ###################################################

  if (length(property) == 56) {
    setnames(property,
             c("property_ID", "listing_title", "property_type",
               "listing_type", "created", "scraped", "country", "latitude",
               "longitude", "region", "city", "zipcode", "neighbourhood",
               "metro_area", "currency", "daily_rate", "daily_rate_native",
               "LTM_revenue", "LTM_revenue_native", "LTM_occupancy",
               "LTM_bookings", "LTM_reserved_days", "LTM_available",
               "LTM_blocked", "bedrooms", "bathrooms", "max_guests",
               "calendar_updated", "response_rate", "superhost",
               "premier_partner", "cancellation", "security_deposit",
               "security_deposit_native", "cleaning_fee", "cleaning_fee_native",
               "extra_people_fee", "extra_people_fee_native", "nightly_rate",
               "monthly_rate", "weekly_rate", "check_in_time", "check_out_time",
               "minimum_stay", "num_reviews", "num_photos", "instant_book",
               "rating", "ab_property", "ab_host", "ab_listing_url",
               "ab_image_url", "ha_property", "ha_host", "ha_listing_url",
               "ha_image_url"))

    if (!keep_cols) {
      helper_progress_message("Dropping extra fields.", .type = "open")

      property[, c("zipcode", "daily_rate", "daily_rate_native", "LTM_revenue",
                   "LTM_revenue_native", "LTM_occupancy", "LTM_bookings",
                   "LTM_reserved_days", "LTM_available", "LTM_blocked",
                   "calendar_updated", "security_deposit_native",
                   "cleaning_fee_native", "extra_people_fee_native",
                   "nightly_rate", "monthly_rate", "weekly_rate",
                   "ab_listing_url", "ab_image_url", "ha_listing_url",
                   "ha_image_url") := NULL]

      helper_progress_message("Extra fields dropped.", .type = "close")

      }
    } else {
      setnames(property,
             c("property_ID", "listing_title", "property_type", "listing_type",
               "created", "scraped", "country", "latitude", "longitude",
               "region", "city", "neighbourhood", "metro_area", "currency",
               "bedrooms", "bathrooms", "max_guests", "response_rate",
               "superhost", "premier_partner", "cancellation",
               "security_deposit", "cleaning_fee", "extra_people_fee",
               "check_in_time", "check_out_time", "minimum_stay", "num_reviews",
               "num_photos", "instant_book", "rating", "ab_property", "ab_host",
               "ha_property", "ha_host"))
    }


  ### Produce error files ######################################################

  helper_progress_message(
    "(1/5) Identifying entries with invalid property_ID or listing_type.",
    .type = "open")

  error <- property[0,]

  if (!is.null(attr(property, "problems"))) {
    error <-
      attr(property, "problems") %>%
      filter(.data$expected != "56 columns") %>%
      pull(.data$row) %>%
      {property[.,]} %>%
      bind_rows(error)
  }

  error <-
    property %>%
    filter(is.na(.data$property_ID) | is.na(.data$listing_type)) %>%
    bind_rows(error)

  error <-
    property %>%
    filter(!str_starts(.data$property_ID, "(ab-)|(ha-)")) %>%
    bind_rows(error)

  property <-
    property %>%
    filter(!property_ID %in% error$property_ID)

  helper_progress_message(
    "(1/5) Entries with invalid property_ID or listing_type identified.",
    .type = "close")

  helper_progress_message("(2/5) Identifying entries with missing geography.",
                          .type = "open")

  missing_geography <-
    property %>%
    filter(is.na(.data$latitude) | is.na(.data$longitude))

  property <-
    property %>%
    filter(!is.na(.data$latitude)) %>%
    filter(!is.na(.data$longitude))

  helper_progress_message("(2/5) Entries with missing geography identified.",
                          .type = "close")


  ### Replace problematic characters ###########################################

  helper_progress_message("(3/5) Removing problematic characters.",
                          .type = "open")

  property <-
    property %>%
    mutate(
      listing_title = str_replace_all(.data$listing_title, "\n|\r|\"|\'", ""))

  helper_progress_message("(3/5) Problematic characters removed.",
                          .type = "close")


  ### Add host_ID field ########################################################

  helper_progress_message("(4/5) Adding new host_ID and housing fields.",
                          .type = "open")

  setDT(property)[, host_ID := as.character(ab_host)
                  ][is.na(ab_host), host_ID := ha_host]


  ### Add housing field ########################################################

  property <-
    property %>%
    strr_housing() %>%
    select(.data$property_ID, .data$host_ID, .data$listing_title:.data$scraped,
           .data$housing, .data$latitude:.data$longitude, .data$country,
           .data$region:.data$ha_host)

  helper_progress_message("(4/5) New host_ID and housing fields added.",
                          .type = "close")


  ### Rename country names to match actual Airbnb usage ########################

  helper_progress_message(
    "(5/5) Harmonizing country names with Airbnb's usage.", .type = "open")

  setDT(property
        )[country == "Bosnia-Herzegovina", country := "Bosnia and Herzegovina"
           ][country == "Brunei Darussalam", country := "Brunei"
             ][country == "C\u00f4te d'Ivoire - Republic of",
               country := "Ivory Coast"
               ][country == "Democratic Republic of Congo",
                 country := "Democratic Republic of the Congo"
               ][country == "Curacao", country := "Cura\u00e7ao"
                 ][country == "Falkland Islands",
                   country := "Falkland Islands (Malvinas)"
                   ][country == "Lao", country := "Laos"
                     ][country == "Macedonia - Republic of",
                       country := "Macedonia"
                       ][country == "Palestine",
                         country := "Palestinian Territories"
                         ][country == "Republic of Congo", country := "Congo"
                           ][country == "Saint-Martin",
                             country := "Saint Martin"
                             ][region == "Bhutan", country := "Bhutan"
                               ][region == "Puerto Rico",
                                 country := "Puerto Rico",
                                 ][region == "United States Virgin Islands",
                                   country := "U.S. Virgin Islands"]

  helper_progress_message(
    "(5/5) Country names harmonized with Airbnb's usage.", .type = "close")


  ### Return output ############################################################

  helper_progress_message("Processing complete.", .type = "final")

  return(list(
    as_tibble(property), as_tibble(error), as_tibble(missing_geography)
    ))

}
