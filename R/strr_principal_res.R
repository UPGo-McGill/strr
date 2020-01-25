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
#' @export

strr_principal_res <- function(property, daily, host, FREH, ghost,
                               start_date = NULL, end_date = NULL,
                               tolerance = 0, field_name = principal_res,
                               quiet = FALSE) {

  ### Error checking and initialization ########################################

  ## TKTK

  start_date <- as.Date(start_date, origin = "1970-01-01")

  end_date <- as.Date(end_date, origin = "1970-01-01")



  ### Set start_date and end_date if they are missing ##########################

  ## TKTK



  ### Set up table #############################################################

    pr_table <- tibble(property_ID = property$property_ID,
                       listing_type = property$listing_type,
                       host_ID = property$host_ID,
                       housing = property$housing)

    pr_ML <-
      daily %>%
      group_by(property_ID) %>%
      summarize(ML = if_else(
        sum(ML * (date >= start_date)) + sum(ML * (date <= end_date)) > 0,
        TRUE, FALSE))

    pr_n <-
      daily %>%
      filter(status %in% c("R", "A"), date >= start_date, date <= end_date) %>%
      count(property_ID, status) %>%
      group_by(property_ID) %>%
      summarize(n_available = sum(n),
                n_reserved = sum(n[status == "R"]))

    pr_table <-
      pr_table %>%
      left_join(pr_ML, by = "property_ID") %>%
      mutate(ML = if_else(is.na(ML), FALSE, ML)) %>%
      left_join(pr_n, by = "property_ID") %>%
      group_by(host_ID, listing_type) %>%
      mutate(LFRML = case_when(
        listing_type != "Entire home/apt" ~ FALSE,
        ML == FALSE                       ~ FALSE,
        n_available == min(n_available)   ~ TRUE,
        TRUE                              ~ FALSE)) %>%
      ungroup()

    pr_table <-
      pr_table %>%
      filter(LFRML == TRUE) %>%
      group_by(host_ID) %>%
      mutate(prob = sample(0:10000, n(), replace = TRUE),
             LFRML = if_else(
               sum(LFRML) > 1 & prob != max(prob), FALSE, LFRML)) %>%
      ungroup() %>%
      select(property_ID, LFRML2 = LFRML) %>%
      left_join(pr_table, ., by = "property_ID") %>%
      mutate(LFRML = if_else(!is.na(LFRML2), LFRML2, LFRML)) %>%
      select(-LFRML2)

    GH_list <-
      GH %>%
      filter(date >= start_date, date <= end_date) %>%
      pull(property_IDs) %>%
      unlist() %>%
      unique()

    pr_table <-
      pr_table %>%
      mutate(GH = if_else(property_ID %in% GH_list, TRUE, FALSE))

    pr_table <-
      FREH %>%
      filter(date >= start_date, date <= end_date) %>%
      group_by(property_ID) %>%
      summarize(FREH = TRUE) %>%
      left_join(pr_table, ., by = "property_ID") %>%
      mutate(FREH = if_else(is.na(FREH), FALSE, FREH))

    # Add principal_res field

    pr_table <-
      pr_table %>%
      mutate({{ field_name }} := case_when(
        housing == FALSE               ~ FALSE,
        GH == TRUE                     ~ FALSE,
        listing_type == "Shared room"  ~ TRUE,
        listing_type == "Private room" ~ TRUE,
        FREH == TRUE                   ~ FALSE,
        LFRML == TRUE                  ~ TRUE,
        ML == TRUE                     ~ FALSE,
        TRUE                           ~ TRUE)) %>%
      select(property_ID, {{ field_name }})

    left_join(property, pr_table, by = "property_ID")

  }
