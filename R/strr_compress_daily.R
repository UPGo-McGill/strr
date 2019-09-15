#' Function to compress raw daily STR tables into UPGo DB format
#'
#' \code{strr_compress_daily} takes raw monthyl daily tables from AirDNA and
#' compresses them into the UPGo database storage format.
#'
#' A function for compressing the daily activity tables supplied each month from
#' AirDNA into a more storage-efficient one-activity-block-per-row format. The
#' function also produces error files which identify possible corrupt or missing
#' lines in the input file.
#'
#' @param daily A daily table in the raw AirDNA format.
#' @param year-month A character string of the format "YYYY-MM" identifying the
#' year and month of the daily table.
#' @param cores A positive integer scalar. How many processing cores should be
#' used to perform the computationally intensive compression step?
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

strr_compress_daily <- function(daily, output_date = NULL, cores = 1) {

  ## Subset daily table and rename fields

  daily <-
    daily %>%
    select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
           `Reservation ID`) %>%
    set_names(c("property_ID", "date", "status", "booked_date", "price",
                "res_ID"))

  ## Find rows with readr errors and add to error file

  error <-
    problems(daily) %>%
    filter(expected != "10 columns", expected != "no trailing characters") %>%
    pull(row) %>%
    daily[.,]

  error_vector <-
    problems(daily) %>%
    filter(expected != "10 columns", expected != "no trailing characters") %>%
    pull(row)

  if (length(error_vector) > 0) {daily <- daily[-error_vector,]}

  ## Find rows with missing property_ID, date or status

  error <-
    daily %>%
    filter(is.na(property_ID) | is.na(date) | is.na(status)) %>%
    rbind(error)

  daily <-
    daily %>%
    filter(!is.na(property_ID), !is.na(date), !is.na(status))

  ## Check status

  error <-
    daily %>%
    filter(!(status %in% c("A", "U", "B", "R"))) %>%
    rbind(error)

  daily <-
    daily %>%
    filter(status %in% c("A", "U", "B", "R"))

  ## Remove duplicate listing entries by price, but don't add to error file

  daily <-
    daily %>%
    filter(!is.na(price))

  ## Find rows with bad currency entries

  currencies <- c("USD", "EUR", "BRL", "DKK", "NZD", "AED", "CAD", "GBP", "PLN",
                  "CHF", "AUD", "CNY", "HKD", "THB", "RUB", "JPY", "ILS", "CZK",
                  "SEK", "HUF", "ZAR", "SGD", "TRY", "MXN", "PHP", "KRW", "ARS",
                  "NOK", "CLP", "INR", "IDR", "UAH", "COP", "TWD", "BGN", "MYR",
                  "HRK", "VND", "RON", "PEN", "SAR", "MAD", "CRC", "UYU", "XPF",
                  NA)

  error <-
    daily %>%
    filter(!(currency %in% currencies)) %>%
    rbind(error)

  daily <-
    daily %>%
    filter(currency %in% currencies)

  ## Find missing rows

  setDT(daily)
  missing_rows <-
    daily[, list(count = .N,
                 full_count = as.integer(max(date) - min(date) + 1)),
          by = property_ID]
  missing_rows[, dif := full_count - count]
  missing_rows <- missing_rows[dif != 0]

  ## Split into list

  daily <-
    daily %>%
    as_tibble() %>%
    mutate(month = month(date), year = year(date))



  ## Output

  return(list(daily_list, error, missing_rows))



  daily_list <- output[[1]]
  write_csv(output[[2]], paste0("output/error_2019_", n, ".csv"))
  write_csv(output[[3]], paste0("output/missing_rows_2019_", n, ".csv"))

  rm(output, daily)

  print(Sys.time())

  compressed <- compress(daily_list)
  save(compressed, file = paste0("output/compressed_2019_", n, ".Rdata"))





  if (cores > 1) {

    daily_list <- split(daily, daily$property_ID)

    daily_list <- map(1:1000, function(i) {
      rbindlist(
        daily_list[(floor(length(daily_list) * (i - 1) / 1000) +
                      1):floor(length(daily_list) * i / 1000)])
    })


  }



  splitter <- function(daily) {

    daily <-
      daily %>%
      group_by(property_ID, status, booked_date, price, currency, res_id, month,
               year) %>%
      summarize(dates = list(date)) %>%
      ungroup()

    single_date <-
      daily %>%
      filter(map(dates, length) == 1) %>%
      mutate(start_date = as.Date(map_dbl(dates, ~{.x}), origin = "1970-01-01"),
             end_date = as.Date(map_dbl(dates, ~{.x}), origin = "1970-01-01")) %>%
      select(property_ID, start_date, end_date, status, booked_date, price,
             currency, res_id)

    one_length <-
      daily %>%
      filter(map(dates, length) != 1,
             map(dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>%
      mutate(start_date = as.Date(map_dbl(dates, min), origin = "1970-01-01"),
             end_date = as.Date(map_dbl(dates, max), origin = "1970-01-01")) %>%
      select(property_ID, start_date, end_date, status, booked_date, price,
             currency, res_id)

    remainder <-
      daily %>%
      filter(map(dates, length) != 1,
             map(dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>%
      mutate(date_range = map(dates, ~{
        tibble(start_date = .x[which(diff(c(0, .x)) > 1)],
               end_date = .x[which(diff(c(.x, 30000)) > 1)])
      })) %>%
      unnest(date_range) %>%
      select(property_ID, start_date, end_date, status, booked_date, price,
             currency, res_id)

    bind_rows(single_date, one_length, remainder) %>%
      arrange(property_ID, start_date)

  }


  compress <- function(daily_list) {
    daily_list %>%
      pbapply::pblapply(splitter, cl = 5) %>%
      do.call(rbind, .)
  }



}
