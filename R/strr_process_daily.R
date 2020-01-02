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
#' @importFrom data.table setDT
#' @importFrom dplyr %>% anti_join bind_rows filter inner_join mutate pull
#' @importFrom dplyr select
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_process_daily <- function(daily, property, quiet = FALSE) {

  time_1 <- Sys.time()

  ## Error checking and initialization

  .datatable.aware = TRUE


  ## Rename fields

  if (length(daily) == 6) {
    daily <-
      daily %>%
      rlang::set_names(c("property_ID", "date", "status", "booked_date",
                         "price", "res_ID"))
  } else if (length(daily) == 10) {
    daily <-
      daily %>%
      rlang::set_names(c("property_ID", "date", "status", "booked_date",
                         "price", "price_native", "currency", "res_ID",
                         "ab_property", "ha_property"))
  } else stop("The `daily` table must have either six or ten fields.")


  ## Find rows with readr errors and add to error file

  if (!quiet) {message("Beginning error check. (",
                       substr(Sys.time(), 12, 19), ")")}

  error_vector <-
    readr::problems(daily) %>%
    filter(.data$expected != "10 columns",
           .data$expected != "no trailing characters") %>%
    pull(row)

  error <- daily[error_vector,]

  if (length(error_vector) > 0) {daily <- daily[-error_vector,]}

  if (!quiet) {message("Initial import errors identified. (",
                       substr(Sys.time(), 12, 19), ")")}


  ## Find rows with missing property_ID, date or status

  error <-
    daily %>%
    filter(is.na(.data$property_ID) | is.na(.data$date) |
             is.na(.data$status)) %>%
    bind_rows(error)

  daily <-
    daily %>%
    filter(!is.na(.data$property_ID), !is.na(.data$date), !is.na(.data$status))

  if (!quiet) {
    message("Rows with missing property_ID, date or status identified. (",
            substr(Sys.time(), 12, 19), ")")}


  ## Check status

  error <-
    daily %>%
    filter(!(.data$status %in% c("A", "U", "B", "R"))) %>%
    bind_rows(error)

  daily <-
    daily %>%
    filter(.data$status %in% c("A", "U", "B", "R"))

  if (!quiet) {message("Rows with invalid status identified. (",
                       substr(Sys.time(), 12, 19), ")")}


  ## Check for listings missing from property file

  error <-
    daily %>%
    anti_join(property, by = "property_ID") %>%
    bind_rows(error)

  daily <-
    daily %>%
    inner_join(select(property, .data$property_ID, .data$host_ID,
                      .data$listing_type, .data$created, .data$scraped,
                      .data$housing, .data$country, .data$region, .data$city),
               by = "property_ID")


  if (!quiet) {message("Rows missing from property file identified. (",
                       substr(Sys.time(), 12, 19), ")")}


  ## Remove duplicate listing entries by price, but don't add to error file

  daily <-
    daily %>%
    filter(!is.na(.data$price))

  if (!quiet) {message("Duplicate rows removed. (",
                       substr(Sys.time(), 12, 19), ")")}


  ## Find missing rows

  .N = property_ID = NULL # due to NSE notes in R CMD check

  setDT(daily)
  missing_rows <-
    daily[, .(count = .N,
              full_count = as.integer(max(date) - min(date) + 1)),
          by = property_ID]

  missing_rows <-
    missing_rows %>%
    as_tibble() %>%
    mutate(dif = .data$full_count - .data$count) %>%
    filter(.data$dif != 0)

  if (!quiet) {message("Missing rows identified. (",
                       substr(Sys.time(), 12, 19), ")")}

  daily <- as_tibble(daily)


  ## Trim entries from property file

  daily_inactive <-
    daily %>%
    filter(.data$date < .data$created | .data$date > .data$scraped) %>%
    select(-.data$created, -.data$scraped)

  daily <-
    daily %>%
    filter(.data$date >= .data$created, .data$date <= .data$scraped) %>%
    select(-.data$created, -.data$scraped)

  if (!quiet) {message("Rows outside active listing period identified. (",
                       substr(Sys.time(), 12, 19), ")")}


  ## Set classes of outputs

  class(daily) <- c(class(daily), "strr_daily")
  class(daily_inactive) <- c(class(daily_inactive), "strr_daily")


  ## Return output

  total_time <- Sys.time() - time_1

  if (!quiet) {message("Processing complete. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (!quiet) {message("Total time: ",
                       substr(total_time, 1, 5), " ",
                       attr(total_time, "units"), ".")}

  return(list(daily, daily_inactive, error, missing_rows))
}
