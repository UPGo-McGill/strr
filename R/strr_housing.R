#' Function to identify STRs located in housing
#'
#' \code{strr_housing} takes an STR property file and adds a `housing` field.
#'
#' A function for adding a `housing` field to an STR property file. The function
#' reads the `property_type` fields and categorizes the values into either
#' housing or not housing.
#'
#' @param property An STR property file.
#' @param property_type The name of a field identifying the type of property a
#' listing is located in.
#' @return The output will be the input `property` table, but with an additional
#' `housing` field added, with TRUE or FALSE values indicating if a given
#' listing is located in housing (TRUE) or in a dedicated tourist accommodation
#' facility such as a hotel or B&B (FALSE).
#' @importFrom rlang .data
#' @export

strr_housing <- function(property, property_type = property_type) {

  helper_check_property(rlang::ensyms(property_type))

  dplyr::mutate(
    property,
    housing =
      dplyr::if_else({{ property_type }} %in% housing_types, TRUE, FALSE),
    # Add extra logic to catch non-housing option with non-ASCII character
    housing = dplyr::if_else(stringr::str_detect({{ property_type }}, "ara/s"),
                             FALSE, .data$housing),
    housing = dplyr::if_else(is.na(.data$housing), TRUE, .data$housing))

  }


