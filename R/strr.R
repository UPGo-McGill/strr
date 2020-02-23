## Define global variables for R CMD check

if(getRversion() >= "2.15.1") {
  utils::globalVariables(c(".", ".I", ".N", ".SD"))
  }
