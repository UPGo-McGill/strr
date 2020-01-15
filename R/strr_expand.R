#' Function to expand compressed STR tables
#'
#' \code{strr_expand} takes an STR file compressed in the UPGo DB format and
#' expands it to a one-row-per-date format.
#'
#' A function for expanding compressed daily activity tables from AirDNA or the
#' ML summary tables UPGo produces, optionally truncating the table by a
#' supplied date range. The function can take advantage of multiprocess and/or
#' remote computation options if a plan is set with the \code{future} package.
#'
#' @param data A table in compressed UPGo DB format (e.g. created by running
#' \code{\link{strr_compress}}). Currently daily activity files and ML daily
#' summary tables are recognized.
#' @param start A character string of format YYYY-MM-DD indicating the
#'   first date to be provided in the output table. If NULL (default), the
#'   earliest date present in the data will be used.
#' @param end A character string of format YYYY-MM-DD indicating the
#'   last date to be provided in the output table. If NULL (default), the
#'   latest date present in the data will be used.
#' @param chunk_size A positive integer scalar. How large should each element be
#' when the table is split for multicore processing? Larger elements should lead
#' to slightly faster processing times but higher memory usage, so low values
#' are recommended for RAM-constrained computers.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A table with one row per date and all other fields returned
#' unaltered.
#' @importFrom dplyr %>% bind_rows filter group_split mutate pull select
#' @importFrom furrr future_map_dfr
#' @importFrom purrr map2
#' @importFrom rlang .data
#' @importFrom stringr str_detect
#' @importFrom tidyr unnest
#' @export

strr_expand <- function(data, start = NULL, end = NULL, chunk_size = 1000,
                        quiet = FALSE) {

  time_1 <- Sys.time()

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  .datatable.aware = TRUE

  # Remove future global export limit

  options(future.globals.maxSize = +Inf)
  on.exit(.Options$future.globals.maxSize <- NULL)

  # Check if table is daily or ML

  if (inherits(data, "strr_daily") | names(data)[1] == "property_ID") {

    helper_progress_message("Daily table identified.")

    daily <- TRUE

  } else if (inherits(data, "strr_multi") | names(data)[1] == "host_ID") {

    helper_progress_message("Multilisting table identified.")

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_multi`.")

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


  ### STORE EXTRA FIELDS AND TRIM DATA #########################################

  ## TKTK Remove 15 once daily DB update is complete
  if (length(data) %in% c(13, 15)) {

    # These fields are per-property
    join_fields <-
      data %>%
      group_by(.data$property_ID) %>%
      filter(.data$start_date == max(.data$start_date)) %>%
      ungroup() %>%
      select(.data$property_ID, .data$host_ID:.data$city)

    # These fields are per-property, per-date
    add_fields <-
      data %>%
      select(.data$property_ID, date = .data$start_date,
             .data$status:.data$res_ID)

    # Keep host_ID, country/region for group_split
    data <-
      data %>%
      select(.data$property_ID, .data$start_date, .data$end_date,
             .data$host_ID, .data$country, .data$region)

  }


  ### PREPARE DATE FIELD AND SPLIT DATA ########################################

  helper_progress_message("Preparing new date field.")

  data <-
    data %>%
    mutate(date = map2(.data$start_date, .data$end_date, ~{.x:.y}))

  helper_progress_message("Splitting data for processing.")

  ## Split by country and region for daily, with host_ID for multi or as backup

  if (daily) {
    data_list <-
      data %>%
      select(-.data$host_ID) %>%
      group_split(.data$country, .data$region, keep = FALSE)

    # Use host_ID for multi
  } else {
    data_list <-
      data %>%
      group_split(.data$host_ID)
  }

  # If a daily file only has a single country, try splitting by host_ID instead
  if (length(data_list) == 1) {
    data_list <-
      data %>%
      select(-.data$country, -.data$region) %>%
      group_split(.data$host_ID, keep = FALSE)
  }

  data_list <-
    data_list %>%
    helper_table_split()


  ### EXPAND TABLE #############################################################

  helper_progress_message("Beginning expansion, using {helper_plan()}.")

  data <-
    data_list %>%
    future_map_dfr(~{

      data.table::setDT(.x)

      .x[, lapply(.SD, unlist), by = 1:nrow(.x)] %>%
        tibble::as_tibble() %>%
        mutate(date = as.Date(.data$date, origin = "1970-01-01")) %>%
        select(-.data$nrow)

      },
      # Suppress progress bar if quiet == TRUE or the plan is remote
      .progress = helper_progress(quiet)
      )


  ### REJOIN TO ADDITIONAL FIELDS, THEN ARRANGE COLUMNS ########################

  helper_progress_message("Joining additional fields to table.")

  if (length(data) == 4) {

    data <-
      data %>%
      arrange(.data$property_ID, .data$date) %>%
      # Join fields which need to be duplicated for specific date ranges
      left_join(add_fields, by = c("property_ID", "date")) %>%
      tidyr::fill(.data$status:.data$res_ID) %>%
      # Join fields which need to be duplicated for specific properties
      left_join(join_fields, by = "property_ID") %>%
      select(.data$property_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date)

  } else {

    data <-
      data %>%
      select(.data$host_ID, .data$date, everything(), -.data$start_date,
             -.data$end_date) %>%
      arrange(.data$host_ID, .data$date)

  }

  ### OPTIONALLY TRIM BASED ON START/END DATE ##################################

  if (!missing(start)) {
    data <- filter(data, .data$date >= start)
  }

  if (!missing(end)) {
    data <- filter(data, .data$date <= end)
  }


  ### SET CLASS OF OUTPUT ######################################################

  if (names(data)[1] == "property_ID") {
    class(data) <- append(class(data), "strr_daily")
  } else {
    class(data) <- append(class(data), "strr_multi")
  }


  ### OUTPUT DATA FRAME ########################################################

  helper_progress_message("Expansion complete.", .final = TRUE)

  return(data)
}
