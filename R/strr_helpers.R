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

  data_list
}

