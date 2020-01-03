#' Function to create daily multilisting tables
#'
#' \code{strr_process_multi} takes processed daily tables and produces summary
#' tables listings host activity per day.
#'
#' A function for cleaning raw daily activity tables from AirDNA and preparing
#' them for compression into the UPGo format. The function also produces error
#' files which identify possible corrupt or missing lines in the input file.
#'
#' @param daily An unprocessed daily table in the raw AirDNA format, with either
#' ten or six fields.
#' @param multiplier An integer scalar. What multiple of the number of available
#' processes should the data be combined into?
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A processed multilisting table, ready for compression with
#' \code{\link{strr_compress}}.
#' @importFrom dplyr %>% anti_join bind_rows filter inner_join mutate pull
#' @importFrom dplyr select
#' @importFrom rlang .data
#' @importFrom tibble as_tibble
#' @export

strr_process_multi <- function(daily, multiplier = 2, quiet = FALSE) {

  time_1 <- Sys.time()

  ## Error checking and initialization

  if (!quiet) {message("Trimming daily table to valid entries. (",
                       substr(Sys.time(), 12, 19), ")")}

  ## Trim daily table

  daily <-
    daily %>%
    filter(.data$status != "U", !is.na(.data$host_ID))


  ## Produce list for processing

  data_list <-
    daily %>%
    group_split(.data$host_ID) %>%
    helper_table_split(multiplier)


  ## Produce multilisting table

  if (!quiet) {message("Beginning processing, using ", helper_plan(), ". (",
                       substr(Sys.time(), 12, 19), ")")}

  multi <-
    data_list %>%
    future_map_dfr(~{
      .x %>%
        count(.data$host_ID, .data$date, .data$listing_type, .data$housing) %>%
        ungroup() %>%
        rename(count = n)
    },
    # Suppress progress bar if quiet == TRUE or the plan is remote
    .progress = helper_progress(quiet))


  ## Return output

  class(multi) <- c(class(multi), "strr_multi")

  total_time <- Sys.time() - time_1

  if (!quiet) {message("Processing complete. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (!quiet) {message("Total time: ",
                       substr(total_time, 1, 5), " ",
                       attr(total_time, "units"), ".")}

  return(multi)
}
