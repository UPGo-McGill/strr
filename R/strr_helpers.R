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
  data_list <- data_list[order(sapply(data_list, nrow), decreasing = TRUE)]

  # Get element nrows
  nrows <- sapply(data_list, nrow)

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

    data_list <- lapply(index_positions, function(x) table[x,])

  } else {
    # If table is sf, use do.call to rbind, to preserve geometry column
    if (sf_flag) {
      data_list <- lapply(index_positions, function(x) {
        do.call(rbind, data_list[x])
      })
      # Otherwise use faster rbindlist
    } else {
      data_list <- lapply(index_positions, function(x) {
        data.table::rbindlist(data_list[x])
        })
    }
  }

  return(data_list)
}


#' Helper function to set a progress reporting strategy
#'
#' \code{handler_strr} sets a progress reporting strategy.
#'
#' @param message A character string describing the task being iterated.
#' @return No visible output.

handler_strr <- function(message) {

  if (requireNamespace("progressr", quietly = TRUE)) {

    if (!requireNamespace("progress", quietly = TRUE)) {

      progressr::handlers("txtprogressbar")

    } else {

      format_string <- paste0(
        message,
        " :current of :total (:tick_rate/s) [:bar] :percent, ETA: :eta")

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
}


#' Helper function to display a message
#'
#' \code{helper_message} produces a formatted progress message.
#' @param ... Character strings to be displayed.
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

    args <- list(...)

    output_time <- paste0(" (", substr(Sys.time(), 12, 19), ")")

    if (requireNamespace("crayon", quietly = TRUE)) {
      output_time <- crayon::cyan(output_time)
    }

    if (.type == "open") {

      if (requireNamespace("crayon", quietly = TRUE)) {
        args <- lapply(args, crayon::silver$italic)
      }

    } else if (.type == "close") {

      if (requireNamespace("crayon", quietly = TRUE)) {
        args <- lapply(args, crayon::silver)
      }

      args <- c("\r", args, output_time, "\n", sep = "")

    } else if (.type == "main") {

      if (requireNamespace("crayon", quietly = TRUE)) {
        args <- lapply(args, crayon::silver)
      }

      args <- c(args, output_time, "\n", sep = "")

    } else if (.type == "final") {

      start_time <- get("start_time", envir = parent.frame(n = 1))
      total_time <- Sys.time() - start_time
      time_final_1 <- substr(total_time, 1, 5)
      time_final_2 <- attr(total_time, 'units')
      total_time <- paste0("Total time: ", time_final_1, " ", time_final_2, ".")

      if (requireNamespace("crayon", quietly = TRUE)) {
        total_time <- crayon::cyan$bold(total_time)
        args <- lapply(args, crayon::silver)
      }

      args <- c(args, output_time, "\n", total_time, "\n", sep = "")

    }

    message(args, appendLF = FALSE)

  }
}


#' Helper function to check a data frame
#'
#' \code{helper_check_data} checks a data frame and verifies that is either
#' strr_daily or strr_host.
#' @return A logical scalar "daily" indicating if the table is strr_daily or
#' strr_host, or an error if the check fails.

helper_check_data <- function() {

  tryCatch({data <- get("data", envir = parent.frame(n = 1))},
           error = function(e) stop("The argument `data` is missing.",
                                    call. = FALSE))

  # Check that daily is a data frame
  if (!inherits(data, "data.frame")) {
    stop("The object supplied to the `data` argument must be a data frame.",
         call. = FALSE)
  }

  # Check if table is daily or host
  if (inherits(data, "strr_daily") | names(data)[1] %in% c("property_ID",
                                                           "Property ID")) {

    daily <- TRUE

  } else if (inherits(data, "strr_host") | names(data)[1] %in% c("host_ID",
                                                                 "Host ID")) {

    daily <- FALSE

  } else stop("Input table must be of class `strr_daily` or `strr_host`.",
              call. = FALSE)

  return(daily)
}


#' Helper function to check an strr_daily data frame
#'
#' \code{helper_check_daily} checks a data frame and verifies that is strr_daily
#' class, and optionally verifies the presence of specified fields in the data
#' frame.
#' @param ... Field names to be passed as quoted symbol names (with
#' `rlang::ensym`).
#' @return An error if the check fails.

helper_check_daily <- function(...) {

  tryCatch({daily <- get("daily", envir = parent.frame(n = 1))},
           error = function(e) stop("The argument `daily` is missing.",
                                    call. = FALSE))

  if (requireNamespace("ellipsis", quietly = TRUE)) {
    ellipsis::check_dots_unnamed()
  }

  # Check that daily is a data frame
  if (!inherits(daily, "data.frame")) {
    stop("The object supplied to the `daily` argument must be a data frame.",
         call. = FALSE)
  }

  # Check that daily is strr_daily
  if (!inherits(daily, "strr_daily") &&
      !names(daily)[1] %in% c("property_ID", "Property ID")) {
    stop("Input table must be of class `strr_daily`.", call. = FALSE)
  }

  ## Check field arguments if any are supplied ---------------------------------

  if (length(list(...)) > 0) {
    lapply(..., function(.x) {
      tryCatch({
        dplyr::pull(daily, !! .x)
      }, error = function(e) {
        stop(paste0("`", rlang::as_string(.x),
                    "` is not a valid field in the input table."),
          call. = FALSE)
      })
    })
  }

  return(NULL)
}


#' Helper function to check a host data frame
#'
#' \code{helper_check_host} checks a data frame and verifies that is strr_host
#' class.
#' @return An error if the check fails.

helper_check_host <- function() {

  tryCatch({host <- get("host", envir = parent.frame(n = 1))},
           error = function(e) stop("The argument `host` is missing.",
                                    call. = FALSE))

  # Check that host is a data frame
  if (!inherits(host, "data.frame")) {
    stop("The object supplied to the `host` argument must be a data frame.",
         call. = FALSE)
  }

  # Check that host is strr_host
  if (!inherits(host, "strr_host") && names(host)[1] != "host_ID") {
    stop("Input table must be of class `strr_host`.", call. = FALSE)
  }
}


#' Helper function to check a property data frame
#'
#' \code{helper_check_property} checks a data frame and verifies that is
#' strr_property class.
#' @param ... Field names to be passed as quoted symbol names (with
#' `rlang::ensym`).
#' @return An error if the check fails.

helper_check_property <- function(...) {

  tryCatch({property <- get("property", envir = parent.frame(n = 1))},
           error = function(e) stop("The argument `property` is missing.",
                                    call. = FALSE))

  if (requireNamespace("ellipsis", quietly = TRUE)) {
    ellipsis::check_dots_unnamed()
  }

  # Check that property is a data frame
  if (!inherits(property, "data.frame")) {
    stop("The object supplied to the `property` argument must be a data frame.",
         call. = FALSE)
  }

  # Check that property is strr_property
  if (!inherits(property, "strr_property") &&
      names(property)[1] != "property_ID") {
    stop("Input table must be of class `strr_property`.", call. = FALSE)
  }

  ## Check field arguments if any are supplied ---------------------------------

  if (length(list(...)) > 0) {
    lapply(..., function(.x) {
      tryCatch({
        dplyr::pull(property, !! .x)
      }, error = function(e) {
        stop(paste0("`", rlang::as_string(.x),
                    "` is not a valid field in the input table."),
          call. = FALSE)
      })
    })
  }

  return(NULL)
}


# #' Helper function to check a review data frame
# #'
# #' \code{helper_check_review} checks a data frame and verifies that is
# #' strr_review class.
# #' @param ... Field names to be passed as quoted symbol names (with
# #' `rlang::ensym`).
# #' @return An error if the check fails.
#
# helper_check_review <- function(...) {
#
#   tryCatch({review <- get("review", envir = parent.frame(n = 1))},
#            error = function(e) stop("The argument `review` is missing.",
#                                     call. = FALSE))
#
#   if (requireNamespace("ellipsis", quietly = TRUE)) {
#     ellipsis::check_dots_unnamed()
#   }
#
#   # Check that review is a data frame
#   if (!inherits(review, "data.frame")) {
#     stop("The object supplied to the `review` argument must be a data frame.",
#          call. = FALSE)
#   }
#
#   # Check that review is strr_review
#   if (!inherits(review, "strr_review") &&
#       names(review)[1] != "review_ID") {
#     stop("Input table must be of class `strr_review`.", call. = FALSE)
#   }
#
#   ## Check field arguments if any are supplied ---------------------------------
#
#   if (length(list(...)) > 0) {
#     lapply(..., function(.x) {
#       tryCatch({
#         dplyr::pull(review, !! .x)
#       }, error = function(e) {
#         stop(paste0("`", rlang::as_string(.x),
#                     "` is not a valid field in the input table."),
#           call. = FALSE)
#       })
#     })
#   }
#
#   return(NULL)
# }


#' Helper function to check a `quiet` argument
#'
#' \code{helper_check_quiet} checks a `quiet` argument and verifies that it is
#' a logical scalar.
#' @return An error if the check fails.

helper_check_quiet <- function() {

  quiet <- get("quiet", envir = parent.frame(n = 1))

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).",
         call. = FALSE)
  }

}
