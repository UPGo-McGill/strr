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
