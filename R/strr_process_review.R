#' Process raw STR review tables into UPGo format
#'
#' \code{strr_process_review} takes raw review tables from AirDNA and cleans
#' them for analysis or for upload in the UPGo database storage format.
#'
#' A function for cleaning raw review tables from AirDNA and preparing them
#' for subsequent analysis or upload in the UPGo format.
#'
#' @param review TKTK
#' @param property An strr_property data frame.
#' @param latest_user A review_user table with a single row containing the most
#' recent observations for each user_ID.
#' @param max_id The highest review_ID currently in use. New review_IDs will be
#' assigned starting with max_id + 1.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return TKTK.
#' @export

strr_process_review <- function(review, property, latest_user = NULL,
                                max_id = 0, quiet = FALSE) {

  ### ERROR CHECKING AND INITIALIZATION ########################################

  start_time <- Sys.time()

  # Input checking
  helper_check_property()
  helper_check_quiet()
  stopifnot(is.numeric(max_id))

  # Silence R CMD check for data.table fields
  property_ID <- drop_cols <- user_ID <- country <- i.country <- user_country <-
    ..keep_cols <- review_ID <- i.region <- i.city <- col_split <- NULL

  # Set data.table threads to match future workers if plan isn't remote
  if (requireNamespace("future", quietly = TRUE) &&
      !"remote" %in% class(future::plan())) {
    threads <- data.table::setDTthreads(future::nbrOfWorkers())
    on.exit({data.table::setDTthreads(threads)}, add = TRUE)
  }


  # ### SET COLUMNS AND COLUMN NAMES #############################################
  #
  # helper_message("(1/4) Cleaning input table.", .type = "open")
  # data.table::setDT(review)
  # drop_cols <- c("Latitude", "Longitude", "Address", "Profile Image URL",
  #                "Profile URL")
  # col_names <- c("property_ID", "date", "review", "user_ID", "member_since",
  #                "user_name", "user_country", "user_region", "user_city",
  #                "description", "school", "work")
  # if (length(review) == 17) review[, c(drop_cols) := NULL]
  # data.table::setnames(review, col_names)
  #
  #
  # ### CREATE REVIEW_ID AND EDIT PROPERTY_ID ####################################
  #
  # data.table::setorder(review, date, property_ID, user_ID)
  # review[, c("review_ID", "property_ID") :=
  #          list(max_id + seq_len(.N), paste0("ab-", property_ID))]
  #
  #
  # ### REMOVE BAD CHARACTERS ####################################################
  #
  # char_cols <- c("review", "user_name", "user_country", "user_region",
  #                "user_city", "description", "school", "work")
  # review[, c(char_cols) := lapply(.SD, function(x) gsub('\\r|\\n|\\"', "", x)),
  #        .SDcols = char_cols]
  #
  #
  # ### DEAL WITH COUNTRY NAMES ##################################################
  #
  # review[data.table::as.data.table(country_match), country := i.country,
  #        on = c("user_country" = "code")]
  # review[, c("user_country", "country") := list(
  #   dplyr::if_else(user_country %in% country_list, user_country, country),
  #   NULL)]
  # helper_message("(1/4) Input table cleaned.", .type = "close")
  #
  #
  # ### CREATE REVIEW_USER #######################################################
  #
  # helper_message("(2/4) Creating review_user table, using ", helper_plan(), ".")
  #
  # # Produce table with one row per distinct combination of user info
  # rm(..keep_cols) # Silence data.table warning about variable in calling scope
  # keep_cols <- c("user_ID", "date", "member_since", "user_name", "user_country",
  #                "user_region", "user_city", "description", "school", "work")
  # review_user <- unique(review[, ..keep_cols], by = setdiff(keep_cols, "date"))
  #
  # # Add latest_user to find duplicates
  # start_date <- min(review_user$date)
  # if (!missing(latest_user)) review_user <-
  #   data.table::rbindlist(list(review_user, latest_user))
  # data.table::setorder(review_user, date, user_ID)
  #
  # # Remove rows that duplicate info already in latest_user
  # review_user <- unique(review, by = setdiff(keep_cols, "date"))[
  #   date >= start_date]
  #
  # # Update latest_user with new entries
  # new_user <- review_user[, .SD[.N], by = user_ID]
  #
  # if (!missing(latest_user)) {
  #
  #   data.table::setDT(latest_user)
  #   latest_user <- latest_user[!user_ID %in% new_user$user_ID]
  #   latest_user <- latest_user_all[, ..keep_cols]
  #   latest_user <- data.table::rbindlist(list(latest_user, new_user))
  #
  # } else latest_user <- new_user
  #
  #
  # ### CREATE REVIEW TEXT #######################################################
  #
  # helper_message("(3/4) Creating review_text table.", .type = "open")
  #
  # review_text <- review[, .(review_ID, date, user_ID, property_ID, review)]
  #
  # helper_message("(3/4) Review_text table created.", .type = "close")
  #
  #
  # ### CREATE REVIEW ############################################################
  #
  # helper_message("(4/4) Creating review table.", .type = "open")
  #
  # drop_cols <- c("review", "member_since", "user_name", "description", "school",
  #                "work")
  #
  # review[, c(drop_cols) := NULL]
  #
  # review[data.table::as.data.table(property),
  #        c("country", "region", "city") := list(i.country, i.region, i.city),
  #        on = "property_ID"]
  #
  # data.table::setcolorder(
  #   review, c("review_ID", "date", "user_ID", "user_country", "user_region",
  #             "user_city"))
  #
  # helper_message("(4/4) Review table created.", .type = "close")
  #
  #
  # ### RETURN OUTPUT ############################################################
  #
  # helper_message("Processing complete.", .type = "final")
  #
  # return(lapply(list(review, review_user, review_text, latest_user),
  #            dplyr::as_tibble))













  # Rename columns ----------------------------------------------------------

  helper_message("(1/4) Cleaning input table.", .type = "open")

  review <-
    review |>
    rename(property_ID = `Property ID`,
           date = `Review Date`,
           review = `Review Text`,
           user_ID = `User ID`,
           member_since = `Member Since`,
           user_name = `First Name`,
           user_country = Country,
           user_region = State,
           user_city = City,
           description = Description,
           school = School,
           work = Work,
           user_image_url = `Profile Image URL`)


  # Create review_ID and edit property_ID -----------------------------------

  review <-
    review |>
    arrange(date, property_ID, user_ID) |>
    mutate(property_ID = paste0("ab-", property_ID),
           review_ID = as.numeric(seq_along(property_ID) + max_id)) |>
    relocate(review_ID)


  # Remove bad characters ---------------------------------------------------

  review <-
    review |>
    mutate(across(where(is.character), str_replace_all, '\\r|\\n|\\"', ""))


  # Deal with countries -----------------------------------------------------

  review <-
    review |>
    left_join(country_match, by = c("user_country" = "code")) |>
    mutate(user_country = if_else(user_country %in% country_list,
                                  user_country, country)) |>
    select(-country)

  helper_message("(1/4) Input table cleaned.", .type = "close")


  # Create review_user ------------------------------------------------------

  helper_message("(2/4) Creating review_user table", .type = "open")

  review_user <-
    review |>
    select(user_ID, start_date = date, member_since, user_name, user_country,
           user_region, user_city, description, school, work, user_image_url) |>
    distinct(across(-start_date), .keep_all = TRUE)

  # Remove duplicates within a single month
  if (length(unique(review_user$user_ID)) != nrow(review_user)) {
    review_user <-
      review_user |>
      group_by(user_ID, start_date) |>
      slice(1) |>
      ungroup()
  }

  # Trim review_user based on latest_user -----------------------------------

  # Create latest_user the first time
  # latest_user <-
  #   review_user |>
  #   group_by(user_ID) |>
  #   filter(start_date == max(start_date)) |>
  #   ungroup()

  # Exclude exact matches with latest_user
  review_user <-
    review_user |>
    anti_join(latest_user, by = setdiff(names(review_user), "start_date"))

  # Add new entries to latest_user
  latest_user <-
    latest_user |>
    filter(!user_ID %in% review_user$user_ID) |>
    bind_rows(review_user) |>
    arrange(user_ID)

  helper_message("(2/4) Review_user table created.", .type = "close")


  # Create review_text ------------------------------------------------------

  helper_message("(3/4) Creating review_text table.", .type = "open")

  review_text <-
    review |>
    select(review_ID, date, user_ID, property_ID, review)

  helper_message("(3/4) Review_text table created.", .type = "close")

  # Create review -----------------------------------------------------------

  helper_message("(4/4) Creating review table.", .type = "open")

  review <-
    review |>
    select(-c(review, member_since, user_name, description, school, work,
              user_image_url)) |>
    left_join(select(property, property_ID, country, region, city),
              by = "property_ID") |>
    select(review_ID, date, user_ID:user_city, property_ID, country:city)

  max_id <- max(review$review_ID)

  helper_message("(4/4) Review table created.", .type = "close")

  helper_message("Processing complete.", .type = "final")

  return(list(review, review_user, review_text, latest_user, max_id))

}
