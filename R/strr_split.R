#' #' Helper function to split tables for multicore processing
#' #'
#' #' \code{strr_split} splits tables into a list of tables suitable for multicore
#' #' processing via \code{plapply}.
#' #'
#' #' A function for splitting tables into a list of tables to allow for convenient
#' #' multicore processing. The function can optionally take a variable which will
#' #' be used to group the
#' #'
#' #' @param .data A data frame.
#' #' @param .split_var The name of a variable to be used to group the list
#' #' elements. Supplying a variable ensures that values of that variable will not
#' #' be spread across multiple list elements. If no variable is supplied (the
#' #' default), the table will be split according to row order.
#' #' @param n_chunks A positive integer scalar. How many elements should the
#' #' output table have? If \code{.split_var} is specified, the value for
#' #' \code{n_chunks} will be capped at the number of unique values of
#' #' \code{.split_var}.
#' #' @param quiet A logical scalar. Should the function execute quietly, or should
#' #' it return status updates throughout the function (default)?
#' #' @return The output will be a list of \code{n_chunks} elements, each of which
#' #' will be a subset of the input table.
#' #' @importFrom dplyr %>% bind_rows group_by group_split
#' #' @importFrom rlang .data
#'
#' strr_split <- function(.data, .split_var = NULL, n_chunks = 10000,
#'                        quiet = FALSE) {
#'
#'   if (!quiet) {message("Splitting table for multicore processing. (",
#'                        substr(Sys.time(), 12, 19), ")")}
#'
#'   if (!missing(.split_var)) {
#'
#'     table_list <-
#'       .data %>%
#'       group_by({{ .split_var }}) %>%
#'       group_split()
#'
#'   }
#'
#'
#'
#'   if (length(daily_list) > n_chunks & chunks == TRUE) {
#'
#'     if (!quiet) {message("Optimizing table pieces. (",
#'                          substr(Sys.time(), 12, 19), ")")}
#'
#'     daily_list <- purrr::map(1:10000, function(i) {
#'       bind_rows(
#'         daily_list[(floor(as.numeric(length(daily_list)) * (i - 1) / 10000) +
#'                       1):floor(as.numeric(length(daily_list)) * i / 10000)])
#'     })}
#'
#'
#' }
