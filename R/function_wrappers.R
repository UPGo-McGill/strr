# Wrapper for progressr::with_progress
#'
#' \code{with_progress} wraps progressr::with_progress if {progressr} is
#' installed, and returns the original expression if not.
#'
#' @param expr An expression to be evaluated.
#' @return The result of evaluating the expression.

with_progress2 <- function(expr) {

  quiet <- get("quiet", envir = parent.frame(n = 1))

  if (!quiet && requireNamespace("progressr", quietly = TRUE)) {

    progressr::with_progress(expr)

  } else expr

}


# Wrapper for progressr::progressor

progressor2 <-

  if (requireNamespace("progressr", quietly = TRUE)) {

    progressr::progressor

  } else function(...) function(...) NULL


# Wrappers for lapply and mapply

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


par_mapply <- function(...) {

  if (requireNamespace("future", quietly = TRUE)) {

    if (requireNamespace("future.apply", quietly = TRUE)) {

      # Overwrite lapply with future.lapply for parallel processing
      future.apply::future_mapply(...)

    } else {

      message("Please install the `future.apply` package to enable ",
              "parallel processing.")

      mapply(...)

    }

  } else mapply(...)

}
