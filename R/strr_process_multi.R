#' Function to create daily multilisting tables
#'
#' \code{strr_process_multi} takes processed daily tables and produces summary
#' tables listings host activity per day.
#'
#' A function for cleaning raw daily activity tables from AirDNA and preparing
#' them for compression into the UPGo format. The function also produces error
#' files which identify possible corrupt or missing lines in the input file.
#'
#' @param .daily An unprocessed daily table in the raw AirDNA format, with
#' either ten or six fields.
#' @param .quiet A logical scalar. Should the function execute quietly, or
#' should it return status updates throughout the function (default)?
#' @param ... Additional arguments to pass to component functions. Arguments
#' must be named. Currently the only valid argument is `multiplier`, to control
#' the number of list elements that will be prepared for parallel processing.
#' The default value is 4, which means the input file will be divided into
#' \code{(4 * #' workers)} number of elements, where `workers` is set in
#' \code{future::plan}. Higher values mean more elements, which will slow down
#' processing time but reduce RAM usage. Lower values will mean the inverse.
#' @return A processed multilisting table, ready for compression with
#' \code{\link{strr_compress}}.
#' @importFrom dplyr %>% anti_join bind_rows filter inner_join mutate pull
#' @importFrom dplyr select
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_process_multi <- function(.daily, .quiet = FALSE, ...) {

  time_1 <- Sys.time()

  ## Error checking and initialization

  # Remove future global export limit
  options(future.globals.maxSize = +Inf)
  on.exit(.Options$future.globals.maxSize <- NULL)


  helper_progress_message("Trimming daily table to valid entries.",
                          .quiet = .quiet)

  ## Trim daily table

  .daily <-
    .daily %>%
    filter(.data$status != "U", !is.na(.data$host_ID)) %>%
    # Remove unnecessary fields to reduce memory usage
    select(.data$host_ID, .data$date, .data$listing_type, .data$housing)

  # Save nrow for final validity check
  daily_check <- nrow(.daily)


  ## Produce list for processing

  data_list <-
    .daily %>%
    group_split(.data$host_ID) %>%
    helper_table_split(...)


  ## Produce multilisting table

  helper_progress_message("Beginning processing, using {helper_plan()}.",
                          .quiet = .quiet)

  multi <-
    data_list %>%
    future_map_dfr(~{
      .x %>%
        count(.data$host_ID, .data$date, .data$listing_type, .data$housing) %>%
        ungroup() %>%
        rename(count = n)
    },
    # Suppress progress bar if .quiet == TRUE or the plan is remote
    .progress = helper_progress(.quiet))


  ## Check validity of output

  if (daily_check != sum(multi$count)) {
    stop("The function did not return the correct number of entries. ",
         "This might be because a parallel worker failed complete its job.")
  }


  ## Return output

  class(multi) <- append(class(multi), "strr_multi")

  helper_progress_message("Processing complete.", .quiet = .quiet)

  helper_total_time(time_1) %>%
    helper_progress_message(.quiet = .quiet, .final = TRUE)

  return(multi)
}
