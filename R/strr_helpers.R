#' Helper function to display furrr progress bars
#'
#' \code{helper_progress} decides whether to display progress bars in furrr
#' functions.
#'
#' A helper function for deciding whether to display a progress bar when a
#' function is called from \code{furrr}, based on the supplied `quiet` argument
#' and the status of the future plan.
#'
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A logical scalar. Should the progress bar be displayed or not?

helper_progress <- function(quiet) {
  tryCatch(
    !("remote" %in% class(future::plan())),
    error = function(e) TRUE) * !quiet
}


#' Helper function to characterize future plan
#'
#' \code{helper_plan} parses the current \code{future::plan} for display in
#' progress messages.
#' @return A character string reporting the current plan.
#' @importFrom future nbrOfWorkers plan

helper_plan <- function() {

  tryCatch({
    workers_number <-
      future::nbrOfWorkers()

    workers_noun <-
      if_else(workers_number == 1, "process", "processes")

    cluster_type <-
      if_else("remote" %in% class(future::plan()), "remote", "local")

    paste0(workers_number, " ", cluster_type, " ", workers_noun)
    },
    error = function(e) "1 local process"
  )


}

#' Helper function to split data for parallel processing
#'
#' \code{helper_table_split} takes a split table and combines it into an
#' optimal number of elements for parallel processing.
#' @param data_list A list of data elements to be resized
#' @param multiplier An integer scalar. What multiple of the number of available
#' processes should the data be combined into?
#' @return A list of data elements.
#' @importFrom dplyr bind_rows
#' @importFrom future nbrOfWorkers

helper_table_split <- function(data_list, multiplier = 4) {

  while (multiplier >= 1) {

    # Try to combine data using initial multiplier value
    if (length(data_list) > multiplier * future::nbrOfWorkers()) {

      data_list <- purrr::map(1:(multiplier * future::nbrOfWorkers()), ~{
        bind_rows(
          data_list[(floor(as.numeric(length(data_list)) * (.x - 1) /
                             (multiplier * future::nbrOfWorkers())) + 1):
                      floor(as.numeric(length(data_list)) * .x /
                              (multiplier * future::nbrOfWorkers()))])
      })

      # Set multiplier to 0 to exit the while-loop
      multiplier <- 0

      # Set multiplier lower and try again
      } else multiplier <- multiplier - 1
  }

  # Order from largest to smallest
  data_list <-
    data_list[order(purrr::map_int(data_list, nrow), decreasing = TRUE)]


  data_list
}


#' Helper function to display a progress message
#'
#' \code{helper_progress_message} produces a formatted progress message,
#' optionally with the current time.
#' @param ... Character strings to be displayed. The strings can include
#' code for evaluation via \code{glue::glue} inside `{}`.
#' @param .quiet The name of the argument in the calling function specifying
#' whether messages should be displayed.
#' @param .final A logical scalar. Is this the final progress message, to be
#' formatted in cyan and bold and display the total time?
#' @return A status message.

helper_progress_message <- function(..., .quiet = quiet, .final = FALSE) {

  ellipsis::check_dots_unnamed()

  if (missing(.quiet)) {
    .quiet <-
      get("quiet", envir = parent.frame(n = 1))
  }

  if (!.quiet) {

    args <- purrr::map(list(...), ~{glue::glue(crayon::silver(.x))})

    output_time <- crayon::cyan(glue::glue(" ({substr(Sys.time(), 12, 19)})"))

    if (!.final) {

      message(args, output_time)

    } else {

      time_1 <- get("time_1", envir = parent.frame(n = 1))
      total_time <- Sys.time() - time_1
      time_final_1 <- substr(total_time, 1, 5)
      time_final_2 <- attr(total_time, 'units')

      message(
        args,
        output_time,
        "\n",
        crayon::bold(crayon::cyan(glue::glue(
          "Total time: {time_final_1} {time_final_2}."))))
      }
  }
}


#' Helper function to calculate the total function time
#'
#' \code{helper_total_time} produces a character string with the total function
#' time.
#' @param time_1 The time when the function began.
#' @return A character string with the total function time.

helper_total_time <- function(time_1) {

  total_time <- Sys.time() - time_1
  time_final_1 <- substr(total_time, 1, 5)
  time_final_2 <- attr(total_time, 'units')
  glue::glue("Total time: {time_final_1} {time_final_2}.")

}


#' Helper function to test for a field
#'
#' \code{helper_test_field} tests for the presence of a given field in the input
#' table.
#' @param data The table to be checked.
#' @param field The field to be checked.
#' @param arg_name A character string supplying the underlying argument name for
#' the field being checked.
#' @return An error if the table is not present.
#' @importFrom rlang as_string ensym

helper_test_field <- function(data, field, arg_name) {

  tryCatch(
    pull(data, {{ field }}),
    error = function(e) {
      stop(glue::glue(
        "The value (`{as_string(ensym(field))}`) supplied to the ",
        "`{arg_name}` argument is not a valid field in the ",
        "input table.")
      )})
}

