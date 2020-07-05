#' Wrapper for progressr::with_progress
#'
#' @param expr An expression to be evaluated.
#' @return The result of evaluating the expression.

with_progress <- function(expr) {

  quiet <- get("quiet", envir = parent.frame(n = 1))

  if (!quiet && requireNamespace("progressr", quietly = TRUE)) {

    progressr::with_progress(expr)

  } else expr

}


# Wrapper for progressr::progressor

progressor <-

  if (requireNamespace("progressr", quietly = TRUE)) {

    progressr::progressor

  } else function(...) function(...) NULL


# Wrapper for lapply

par_lapply <- function(...) {

  if (requireNamespace("future", quietly = TRUE)) {

    if (requireNamespace("future.apply", quietly = TRUE)) {

      # Overwrite lapply with future.lapply for parallel processing
      future.apply::future_lapply(...)

      } else {

        message("Please install the `future.apply` package to enable ",
                "parallel processing.")

        lapply(...)

      }

    } else lapply(...)

}


# Wrapper for future assignment

`%<-%` <- if (requireNamespace("future", quietly = TRUE) &&
              "remote" %in% class(future::plan())) future::`%<-%` else `<-`


