#' Helper function to display furrr progress bars
#'
#' \code{helper_progress} decides whether to display progress bars in furrr
#' functions.
#'
#' A helper function for deciding whether to display a progress bar when a
#' function is called from \code{furrr}, based on the `quiet` argument supplied
#' to the calling function and on the status of the future plan.
#'
#' @return A logical scalar. Should the progress bar be displayed or not?

helper_progress <- function() {

  quiet <- get("quiet", envir = parent.frame(n = 1))

  tryCatch(
    !("remote" %in% class(future::plan())),
    error = function(e) TRUE) * !quiet
}


#' Helper function to characterize future plan
#'
#' \code{helper_plan} parses the current \code{future::plan} for display in
#' progress messages.
#' @return A character string reporting the current plan.

helper_plan <- function() {

  if (requireNamespace("future", quietly = TRUE)) {

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

  } else "1 local process"

}

#' Helper function to split data for parallel processing
#'
#' \code{helper_table_split} takes a split table and combines it into an
#' optimal number of elements for parallel processing.
#' @param data_list A list of data elements to be resized
#' @param multiplier An integer scalar. What multiple of the number of available
#' processes should the data be combined into?
#' @param type A character scalar, either ".list" if the input is a list of data
#' frames or, if the input is a single data frame, the name of a variable
#' containing nested data frames.
#' @return A list of data elements.

helper_table_split <- function(data_list, multiplier = 10, type = ".list") {

  if (type != ".list") {

    table <- data_list
    data_list <- table[[type]]

  }

  sf_flag <- inherits(data_list[[1]], "sf")

  # Get target number of elements
  if (requireNamespace("future", quietly = TRUE)) {
    n_elements <- min(length(data_list), multiplier * future::nbrOfWorkers())
  } else n_elements <- min(length(data_list), multiplier)

  # Initialize list of index positions
  index_positions <- list()
  length(index_positions) <- n_elements

  # Order from largest to smallest
  data_list <-
    data_list[order(purrr::map_int(data_list, nrow), decreasing = TRUE)]

  # Get element nrows
  nrows <- purrr::map_int(data_list, nrow)

  # Set initial target nrow
  target_nrow <- sum(nrows) / n_elements

  for (i in seq_len(n_elements)) {

    index_positions[[i]] <- i

    # Add shortest list nrow to longest until target nrow is reached
    while (nrows[i] < target_nrow) {

      nrows[i] <- nrows[i] + nrows[length(nrows)]

      index_positions[[i]] <- c(index_positions[[i]], length(nrows))

      nrows <- nrows[-length(nrows)]
    }

    # Update the target nrow for remaining list elements
    target_nrow <- sum(nrows[-(1:i)]) / (n_elements - i)

  }

  # Deal with case where input is nested data frame
  if (type != ".list") {

    data_list <-
      purrr::map(index_positions, ~table[.x,])

  } else {
    # If table is sf, use do.call to rbind, to preserve geometry column
    if (sf_flag) {
      data_list <- purrr::map(index_positions, ~{
        do.call(rbind, data_list[.x])
      })
      # Otherwise use faster rbindlist
    } else {
      data_list <-
        map(index_positions, ~data.table::rbindlist(data_list[.x]))
    }
  }

  return(data_list)
}


#' Helper function to display a progress message
#'
#' \code{helper_progress_message} produces a formatted progress message,
#' optionally with the current time.
#' @param ... Character strings to be displayed. The strings can include
#' code for evaluation via \code{glue::glue} inside `{}`.
#' @param .type One of c("open", "close", "main", "plan", "final"). "Open"
#' prints a temporary message in grey italics with no timestamp. "Close" is
#' designed to be called after "open", since it overwrites the previous line
#' with a message in grey with a timestamp in cyan. "Main" is the same as
#' "close" but does not override the previous line. "Progress" adds a newline
#' after the output so that a subsequent progress bar does not overwrite it.
#' "Final" is the same as "main" but appends an additional line in cyan bold
#' which states the total time.
#' @param .quiet The name of the argument in the calling function specifying
#' whether messages should be displayed.
#' @return A status message.

helper_progress_message <- function(..., .type = "main", .quiet = NULL) {

  if (requireNamespace("ellipsis", quietly = TRUE)) {
    ellipsis::check_dots_unnamed()
  }

  if (missing(.quiet)) {
    .quiet <-
      get("quiet", envir = parent.frame(n = 1))
  }

  if (!.quiet) {

    args <- purrr::map(list(...), ~glue::glue(.x))

    output_time <- crayon::cyan(glue::glue(" ({substr(Sys.time(), 12, 19)})"))


    if (.type == "open") {

      args <- purrr::map(args, crayon::silver$italic)
      args <- c("\n", args, sep = "")

    } else if (.type == "close") {

      args <- purrr::map(args, crayon::silver)
      args <- c("\r", args, output_time, sep = "")

    } else if (.type == "main") {

      args <- purrr::map(args, crayon::silver)
      args <- c("\n", args, output_time, sep = "")

    } else if (.type == "progress") {

      args <- purrr::map(args, crayon::silver)
      args <- c("\n", args, output_time, "\n\n", sep = "")

    } else if (.type == "final") {

      start_time <- get("start_time", envir = parent.frame(n = 1))
      total_time <- Sys.time() - start_time
      time_final_1 <- substr(total_time, 1, 5)
      time_final_2 <- attr(total_time, 'units')

      total_time <- crayon::cyan$bold(glue::glue(
        "Total time: {time_final_1} {time_final_2}."
      ))

      args <- purrr::map(args, crayon::silver)
      args <- c("\n", args, output_time, "\n", total_time, "\n", sep = "")

    }

    message(args, appendLF = FALSE)

  }
}


#' Helper function to set a progress reporting strategy
#'
#' \code{handler_strr} sets a progress reporting strategy.
#'
#' @param message A character string describing the task being iterated.
#' @return No visible output.

handler_strr <- function(message) {

  if (requireNamespace("progressr", quietly = TRUE)) {

    format_string <- paste0(
      message, " :current of :total (:tick_rate/s) [:bar] :percent, ETA: :eta")

    if (requireNamespace("crayon", quietly = TRUE)) {
      progressr::handlers(
        progressr::handler_progress(
          format = crayon::silver(crayon::italic(format_string)),
          show_after = 0
        ))
    } else {
      progressr::handlers(
        progressr::handler_progress(
          format = format_string,
          show_after = 0
        ))
    }
  }
}


#' Helper function to display a message
#'
#' \code{helper_message} produces a formatted progress message.
#' @param ... Character strings to be displayed. The strings can include
#' code for evaluation via \code{glue::glue} inside `{}`.
#' @param .type One of c("open", "close", "main", "final"). "Open" prints a
#' temporary message in grey italics with no timestamp. "Close" is designed to
#' be called after "open", since it overwrites the previous line with a message
#' in grey with a timestamp in cyan. "Main" is the same as "close" but does not
#' override the previous line. "Final" is the same as "main" but appends an
#' additional line in cyan bold which states the total time.
#' @param .quiet The name of the argument in the calling function specifying
#' whether messages should be displayed.
#' @return A status message.

helper_message <- function(..., .type = "main", .quiet = NULL) {

  if (requireNamespace("ellipsis", quietly = TRUE)) {
    ellipsis::check_dots_unnamed()
  }

  if (missing(.quiet)) {
    .quiet <-
      get("quiet", envir = parent.frame(n = 1))
  }

  if (!.quiet) {

    args <- purrr::map(list(...), ~glue::glue(.x))

    output_time <- glue::glue(" ({substr(Sys.time(), 12, 19)})")

    if (requireNamespace("crayon", quietly = TRUE)) {
      output_time <- crayon::cyan(output_time)
    }

    if (.type == "open") {

      if (requireNamespace("crayon", quietly = TRUE)) {
        args <- purrr::map(args, crayon::silver$italic)
      }

    } else if (.type == "close") {

      if (requireNamespace("crayon", quietly = TRUE)) {
        args <- purrr::map(args, crayon::silver)
      }

      args <- c("\r", args, output_time, "\n", sep = "")

    } else if (.type == "main") {

      if (requireNamespace("crayon", quietly = TRUE)) {
        args <- purrr::map(args, crayon::silver)
      }

      args <- c(args, output_time, "\n", sep = "")

    } else if (.type == "final") {

      start_time <- get("start_time", envir = parent.frame(n = 1))
      total_time <- Sys.time() - start_time
      time_final_1 <- substr(total_time, 1, 5)
      time_final_2 <- attr(total_time, 'units')
      total_time <- glue::glue("Total time: {time_final_1} {time_final_2}.")

      if (requireNamespace("crayon", quietly = TRUE)) {
        total_time <- crayon::cyan$bold(total_time)
        args <- purrr::map(args, crayon::silver)
      }

      args <- c(args, output_time, "\n", total_time, "\n", sep = "")

    }

    message(args, appendLF = FALSE)

  }
}

