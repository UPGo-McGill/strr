
<!-- README.md is generated from README.Rmd. Please edit that file -->

# strr

<!-- badges: start -->

[![R build
status](https://github.com/UPGo-McGill/strr/workflows/R-CMD-check/badge.svg)](https://github.com/UPGo-McGill/strr/actions)
[![codecov](https://codecov.io/gh/UPGo-McGill/strr/branch/master/graph/badge.svg)](https://codecov.io/gh/UPGo-McGill/strr)

<!-- badges: end -->

Tools for analysis of short-term rental data.

  - Clean and process raw AirDNA data files.
  - Compress and expand tables of STR data.
  - Generate reliable statistical estimates of STR listings’ actual
    locations.
  - Identify commercial STR operations.
  - Identify frequently rented entire-home listings.
  - Identify “ghost hostels”–clusters of private-room listings operated
    out of a single housing unit.
  - Identify STR listings which are located in actual housing units.

## Installation

You can install the released version of strr from
[CRAN](https://CRAN.R-project.org) with (This is not currently working,
since the package has not yet been submitted to CRAN.):

``` r
install.packages("strr")
```

And the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("UPGo-McGill/strr")
```

## strr\_process\_\*: Cleaning and processing raw AirDNA data files

TKTK

``` r
library(strr)
## basic example code
```

## strr\_multi: Identifying commercial STR operations

TKTK

## strr\_compress and strr\_expand: Compressing and expanding tables of STR data

TKTK

## strr\_raffle: Generating reliable estimates of STR listing locations

TKTK

## strr\_FREH: Identifying frequently rented entire-home STRs

TKTK

## strr\_ghost: Identifying clusters of private-room listings operated out of a single housing unit

TKTK

## strr\_housing: Identifying STR listings located in actual housing units

TKTK

## Data usage

Most of the functions in strr are designed to work with raw data from
AirDNA: either the `property` files which contain one entry per listing,
or the `daily` files which contain one entry per listing per day. TKTK
