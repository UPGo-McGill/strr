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
#' @importFrom dplyr %>% bind_rows case_when filter if_else mutate pull select
#' @importFrom rlang .data set_names
#' @importFrom stringr str_replace_all str_starts
#' @export

strr_process_property <- function(property, keep_cols = FALSE, quiet = FALSE) {

  time_1 <- Sys.time()

  ### Error checking and initialization ########################################

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


  ### Trim and rename fields ###################################################

  if (length(property) == 56 & keep_cols) {
    property <-
      property %>%
      set_names(c(
        "property_ID", "listing_title", "property_type",
        "listing_type", "created", "scraped",
        "country", "latitude", "longitude",
        "region", "city", "zipcode",
        "neighbourhood", "metro_area", "currency",
        "daily_rate", "daily_rate_native", "LTM_revenue",
        "LTM_revenue_native", "LTM_occupancy", "LTM_bookings",
        "LTM_reserved_days", "LTM_available", "LTM_blocked",
        "bedrooms", "bathrooms", "max_guests",
        "calendar_updated", "response_rate", "superhost",
        "premier_partner", "cancellation", "security_deposit",
        "security_deposit_native", "cleaning_fee", "cleaning_fee_native",
        "extra_people_fee", "extra_people_fee_native", "nightly_rate",
        "monthly_rate", "weekly_rate", "check_in_time",
        "check_out_time", "minimum_stay", "num_reviews",
        "num_photos", "instant_book", "rating",
        "ab_property", "ab_host", "ab_listing_url",
        "ab_image_url", "ha_property", "ha_host",
        "ha_listing_url", "ha_image_url"))
  } else {

    if (length(property) == 56 & !keep_cols) {

      helper_progress_message("Dropping extra fields.")

      property <-
        property %>%
        # Select correct columns with index positions
        select(1:11, 13:15, 25:27, 29:33, 35, 37, 42:50, 53:54)
    }

    property <-
      property %>%
      set_names(c("property_ID", "listing_title", "property_type",
                  "listing_type", "created", "scraped", "country", "latitude",
                  "longitude", "region", "city", "neighbourhood", "metro_area",
                  "currency", "bedrooms", "bathrooms", "max_guests",
                  "response_rate", "superhost", "premier_partner",
                  "cancellation", "security_deposit", "cleaning_fee",
                  "extra_people_fee", "check_in_time", "check_out_time",
                  "minimum_stay", "num_reviews", "num_photos", "instant_book",
                  "rating", "ab_property", "ab_host", "ha_property", "ha_host"))
    }


  ### Produce error files ######################################################

  helper_progress_message("Beginning error check.")

  error <-
    readr::problems(property) %>%
    filter(.data$expected != "56 columns") %>%
    pull(.data$row) %>%
    {property[.,]}

  if (nrow(error) > 0) {
    property <-
      readr::problems(property) %>%
      filter(.data$expected != "56 columns") %>%
      pull(.data$row) %>%
      {property[-.,]}
  }

  error <-
    property %>%
    filter(is.na(.data$property_ID) | is.na(.data$listing_type)) %>%
    bind_rows(error)

  property <-
    property %>%
    filter(!is.na(.data$property_ID), !is.na(.data$listing_type))

  error <-
    property %>%
    filter(!str_starts(.data$property_ID, "ab-"),
           !str_starts(.data$property_ID, "ha-")) %>%
    bind_rows(error)

  property <-
    property %>%
    filter(str_starts(.data$property_ID, "ab-") |
             str_starts(.data$property_ID, "ha-"))

  missing_geography <-
    property %>%
    filter(is.na(.data$latitude) | is.na(.data$longitude))

  property <-
    property %>%
    filter(!is.na(.data$latitude), !is.na(.data$longitude))


  ### Replace problematic characters ###########################################

  helper_progress_message("Cleaning entries and adding fields.")

  property <-
    property %>%
    mutate(listing_title = str_replace_all(
      .data$listing_title, c('\n' = "", '\r' = "", '\"' = "", "\'" = "")))


  ### Add host_ID field ########################################################

  property <-
    property %>%
    mutate(host_ID = if_else(!is.na(.data$ab_host),
                             as.character(.data$ab_host),
                             .data$ha_host))


  ### Add housing field ########################################################

  property <-
    property %>%
    strr_housing() %>%
    select(.data$property_ID, .data$host_ID, .data$listing_title:.data$scraped,
           .data$housing, .data$latitude:.data$longitude, .data$country,
           .data$region:.data$ha_host)


  ### Rename country names to match actual Airbnb usage ########################

  property <-
    property %>%
    mutate(country = case_when(
      .data$country == "Bosnia-Herzegovina"     ~ "Bosnia and Herzegovina",
      .data$country == "Brunei Darussalam"      ~ "Brunei",
      .data$country == "C\u00f4te d'Ivoire - Republic of" ~ "Ivory Coast",
      .data$country == "Curacao"                ~ "Cura\u00e7ao",
      .data$country == "Falkland Islands"       ~ "Falkland Islands (Malvinas)",
      .data$country == "Lao"                    ~ "Laos",
      .data$country == "Macedonia - Republic of" ~ "Macedonia",
      .data$country == "Palestine"              ~ "Palestinian Territories",
      .data$country == "Republic of Congo"      ~ "Congo",
      .data$country == "Saint-Martin"           ~ "Saint Martin",
      .data$region  == "Bhutan"                 ~ "Bhutan",
      .data$region  == "Puerto Rico"            ~ "Puerto Rico",
      .data$region  == "United States Virgin Islands" ~ "U.S. Virgin Islands",
      TRUE ~ .data$country
    ))

  ### Return output ############################################################

  helper_progress_message("Processing complete.", .final = TRUE)

  return(list(property, error, missing_geography))

}