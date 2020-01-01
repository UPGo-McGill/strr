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

  workers_number <-
    future::nbrOfWorkers()

  workers_noun <-
    if_else(workers_number == 1, "process", "processes")

  cluster_type <-
    if_else("remote" %in% class(future::plan()), "remote", "local")

  paste0(workers_number, " ", workers_noun, " in a ", cluster_type, " cluster")

}


helper_plan()
