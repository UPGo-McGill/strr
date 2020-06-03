#' Function to identify STR listings which are likely to be principal residences
#'
#' \code{strr_principal_res} takes a set of inputs about listing activity and
#' identifies listings which are likely to be principal residences in a given
#' time frame.
#'
#' TKTK explanation
#'
#' @param property A data frame of STR listings in standard UPGo format.
#' @param daily A data frame of daily STR activity in standard UPGo format.
#' @param host A data frame of STR host activity in standard UPGo format.
#' @param FREH A data frame with frequently rented entire-home listing status by
#' day, of the type produced with \code{\link{strr_FREH}}.
#' @param ghost A data frame with ghost hostel activity by day, of the type
#' produced with \code{\link{strr_ghost}}
#' @param start_date A character string of format YYYY-MM-DD indicating the
#'   first date for which to assess principal residence status. If NULL
#'   (default), the earliest date common to the `daily`, `host`, `FREH` and
#'   `ghost` tables will be used.
#' @param end_date A character string of format YYYY-MM-DD indicating the last
#' date for which to assess principal residence status. If NULL (default), the
#' latest date common to the `daily`, `host`, `FREH` and `ghost` tables will be
#' used.
#' @param tolerance A number between 0 (default) and 1 setting the tolerance
#' with which daily multilisting, FREH and ghost hostel statuses should be
#' converted into a single assessment of these statuses across the entire date
#' range. A value of, e.g., 0.1 means that if a listing has FREH status on more
#' than 10% of dates in the date range, it is considered an FREH listing for
#' the purpose of setting principal residence status. A value of 1 means that
#' the listing must be FREH on every single date in the date range to be
#' considered FREH for the purpose of setting principal residence status, while
#' a value of 0 means that if the listing is FREH on any number of dates greater
#' than 0 then it will be considered FREH for the purpose of setting principal
#' residence status.
#' @param field_name A character string to name the new logical field
#' identifying principal residences in the output table. The default is
#' `principal_res`.
#' @param quiet A logical vector. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be the `property` input table with one additional
#' logical field (with name taken from the `field_name` argument) indicating
#' principal residence status.
#' @importFrom rlang .data

strr_principal_res <- function(property, daily, host, FREH, ghost,
                               start_date = NULL, end_date = NULL,
                               tolerance = 0, field_name = principal_res,
                               quiet = FALSE) {

  ### Error checking and initialization ########################################

  principal_res <- NULL

  ## TKTK

  start_date <- as.Date(start_date, origin = "1970-01-01")

  end_date <- as.Date(end_date, origin = "1970-01-01")



  ### Set start_date and end_date if they are missing ##########################

  ## TKTK



  ### Set up table #############################################################

    pr_table <- dplyr::tibble(property_ID = property$property_ID,
                       listing_type = property$listing_type,
                       host_ID = property$host_ID,
                       housing = property$housing)

    pr_ML <-
      daily %>%
      dplyr::group_by(.data$property_ID) %>%
      dplyr::summarize(ML = if_else(
        sum(.data$ML * (.data$date >= start_date)) +
          sum(.data$ML * (.data$date <= end_date)) > 0,
        TRUE, FALSE))

    pr_n <-
      daily %>%
      dplyr::filter(.data$status %in% c("R", "A"),
             .data$date >= start_date,
             .data$date <= end_date) %>%
      dplyr::count(.data$property_ID, .data$status) %>%
      dplyr::group_by(.data$property_ID) %>%
      dplyr::summarize(n_available = sum(.data$n),
                n_reserved = sum(.data$n[.data$status == "R"]))

    pr_table <-
      pr_table %>%
      dplyr::left_join(pr_ML, by = "property_ID") %>%
      dplyr::mutate(ML = dplyr::if_else(is.na(.data$ML), FALSE, .data$ML)) %>%
      dplyr::left_join(pr_n, by = "property_ID") %>%
      dplyr::group_by(.data$host_ID, .data$listing_type) %>%
      dplyr::mutate(LFRML = dplyr::case_when(
        .data$listing_type != "Entire home/apt"       ~ FALSE,
        .data$ML == FALSE                             ~ FALSE,
        .data$n_available == min(.data$n_available)   ~ TRUE,
        TRUE                                          ~ FALSE)) %>%
      dplyr::ungroup()

    pr_table <-
      pr_table %>%
      dplyr::filter(.data$LFRML == TRUE) %>%
      dplyr::group_by(.data$host_ID) %>%
      dplyr::mutate(prob = sample(0:10000, dplyr::n(), replace = TRUE),
             LFRML = dplyr::if_else(
               sum(.data$LFRML) > 1 & .data$prob != max(.data$prob), FALSE,
               .data$LFRML)) %>%
      dplyr::ungroup() %>%
      dplyr::select(.data$property_ID, LFRML2 = .data$LFRML) %>%
      dplyr::left_join(pr_table, ., by = "property_ID") %>%
      dplyr::mutate(LFRML = dplyr::if_else(!is.na(.data$LFRML2), .data$LFRML2, .data$LFRML)
             ) %>%
      dplyr::select(-.data$LFRML2)

    GH_list <-
      ghost %>%
      dplyr::filter(.data$date >= start_date, .data$date <= end_date) %>%
      dplyr::pull(.data$property_IDs) %>%
      unlist() %>%
      unique()

    pr_table <-
      pr_table %>%
      dplyr::mutate(GH = dplyr::if_else(.data$property_ID %in% GH_list, TRUE, FALSE))

    pr_table <-
      FREH %>%
      dplyr::filter(.data$date >= start_date, .data$date <= end_date) %>%
      dplyr::group_by(.data$property_ID) %>%
      dplyr::summarize(FREH = TRUE) %>%
      dplyr::left_join(pr_table, ., by = "property_ID") %>%
      dplyr::mutate(FREH = dplyr::if_else(is.na(.data$FREH), FALSE, .data$FREH))

    # Add principal_res field

    pr_table <-
      pr_table %>%
      dplyr::mutate({{ field_name }} := dplyr::case_when(
        .data$housing == FALSE               ~ FALSE,
        .data$GH == TRUE                     ~ FALSE,
        .data$listing_type == "Shared room"  ~ TRUE,
        .data$listing_type == "Private room" ~ TRUE,
        .data$FREH == TRUE                   ~ FALSE,
        .data$LFRML == TRUE                  ~ TRUE,
        .data$ML == TRUE                     ~ FALSE,
        TRUE                                 ~ TRUE)) %>%
      dplyr::select(.data$property_ID, {{ field_name }})

    dplyr::left_join(property, pr_table, by = "property_ID")

  }
