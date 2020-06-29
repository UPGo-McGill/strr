.onAttach <- function(libname, pkgname) {

  if (!requireNamespace("future", quietly = TRUE) ||
      !requireNamespace("furrr",  quietly = TRUE)) {

    packageStartupMessage("Install the {future} and {furrr} packages to ",
                          "enable multithreaded processing.")

  }

  if (!requireNamespace("progressr", quietly = TRUE)) {

    packageStartupMessage("Install the {progressr} package to ",
                          "enable progress bars.")

  }

  if (!requireNamespace("crayon", quietly = TRUE)) {

    packageStartupMessage("Install the {crayon} package to ",
                          "enable styled output text.")

  }

}


.onLoad <- function(libname, pkgname) {

  if (requireNamespace("future", quietly = TRUE)) {

    .strr_env$globals_max_size <- options(future.globals.maxSize = +Inf)

  }

}

.onUnload <- function(libname, pkgname) {

  if (requireNamespace("future", quietly = TRUE)) {

    options(future.globals.maxSize = .strr_env$globals_max_size)

  }

}
