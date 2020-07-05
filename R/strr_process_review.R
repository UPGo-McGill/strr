#' Process raw STR review tables into UPGo format
#'
#' \code{strr_process_review} takes raw review tables from AirDNA and cleans
#' them for analysis or for upload in the UPGo database storage format.
#'
#' A function for cleaning raw review tables from AirDNA and preparing them
#' for subsequent analysis or upload in the UPGo format.
#'
#' @param review TKTK
#' @param property TKTK
#' @param latest_user TKTK
#' @param max_id The highest review_ID currently in use. New review_IDs will be
#' assigned starting with max_id + 1.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return A list with three elements: 1) the processed property table, with 35
#' fields; 2) an error table identifying corrupt or otherwise invalid row
#' entries; 3) a missing_geography table identifying property_IDs with missing
#' latitude/longitude coordinates.
#' @export

strr_process_review <- function(review, property, latest_user, max_id = 0,
                                quiet = FALSE) {

  ### ERROR CHECKING AND INITIALIZATION ########################################

  start_time <- Sys.time()


  ## Input checking ------------------------------------------------------------

  helper_check_property()
  helper_check_quiet()


  ## Prepare data.table and future variables -----------------------------------

  # Silence R CMD check for data.table fields
  property_ID <- ..drop_cols <- user_ID <- country <- i.country <-
    user_country <- ..keep_cols <- review_ID <- i.region <- i.city <-
    col_split <- NULL

  # Set data.table threads to match future workers if plan isn't remote
  if (requireNamespace("future", quietly = TRUE) &&
      !"remote" %in% class(future::plan())) {

    threads <- data.table::setDTthreads(future::nbrOfWorkers())

    on.exit({data.table::setDTthreads(threads)}, add = TRUE)

  }


  ### SET COLUMNS AND COLUMN NAMES #############################################

  helper_message("(1/4) Cleaning input table.", .type = "open")

  data.table::setDT(review)

  drop_cols <- c("Latitude", "Longitude", "Address", "Profile Image URL",
                 "Profile URL")

  if (length(review) == 17) review[, ..drop_cols := NULL]

  data.table::setnames(review, c(
    "property_ID", "date", "review", "user_ID", "member_since", "user_name",
    "user_country", "user_region", "user_city", "description", "school", "work")
    )


  ### CREATE REVIEW_ID AND EDIT PROPERTY_ID ####################################

  data.table::setorder(review, date, property_ID, user_ID)

  review[, c("review_ID", "property_ID") := list(max_id + seq_len(.N),
                                                 paste0("ab-", property_ID))]


  ### REMOVE BAD CHARACTERS ####################################################

  char_cols <-
    c("review", "user_name", "user_country", "user_region", "user_city",
      "description", "school", "work")

  review[, c(char_cols) := lapply(.SD, function(x) gsub('\\r|\\n|\\"', "", x)),
         .SDcols = char_cols]


  ### DEAL WITH COUNTRY NAMES ##################################################

  review[data.table::as.data.table(country_match), country := i.country,
         on = c("user_country" = "code")]

  review[, c("user_country", "country") := list(
    if_else(user_country %in% country_list, user_country, country),
    NULL)]

  helper_message("(1/4) Input table cleaned.", .type = "close")


  ### CREATE REVIEW_USER #######################################################

  helper_message("(2/4) Creating review_user table, using {helper_plan()}")


  ## Produce table with one row per distinct combination of user info ----------

  rm(..keep_cols) # Silence data.table warning about variable in calling scope

  keep_cols <- c("user_ID", "date", "member_since", "user_name", "user_country",
                 "user_region", "user_city", "description", "school", "work")

  review_user <- unique(review[, ..keep_cols], by = setdiff(keep_cols, "date"))


  ## Add latest_user to find duplicates ----------------------------------------

  start_date <- min(review_user$date)

  review_user <- data.table::rbindlist(list(review_user, latest_user))

  data.table::setorder(review_user, date, user_ID)


  ## Split table for computation-intensive steps -------------------------------

  review_user[, col_split := ceiling(user_ID / 100000)]

  data_list <- split(review_user, by = "col_split", keep.by = FALSE)


  ## Remove rows that duplicate info already in latest_user --------------------

  handler_strr("Analyzing duplicates: row")

  with_progress({

    .strr_env$pb <- progressor(steps = nrow(review_user))

    data_list <- par_lapply(data_list, function(.x) {
      .strr_env$pb(amount = nrow(.x))

      unique(.x, by = setdiff(keep_cols, "date"))[date >= start_date]

      })

    })

  review_user <- data.table::rbindlist(data_list)


  ## Update latest_user with new entries ---------------------------------------

  handler_strr("Adding new entries to latest_user: row")

  with_progress({

    .strr_env$pb <- progressor(steps = sum(sapply(data_list, nrow)))

    new_user <- par_lapply(data_list, function(.x) {

      .strr_env$pb(amount = nrow(.x))

      .x[, .SD[.N], by = user_ID]

      })

    })

  new_user <- data.table::rbindlist(new_user)

  data.table::setDT(latest_user)

  latest_user <- latest_user[!user_ID %in% new_user$user_ID]

  latest_user <- data.table::rbindlist(list(latest_user, new_user))


  ### CREATE REVIEW TEXT #######################################################

  helper_message("(3/4) Creating review_text table.", .type = "open")

  review_text <- review[, .(review_ID, date, user_ID, property_ID, review)]

  helper_message("(3/4) Review_text table created.", .type = "close")


  ### CREATE REVIEW ############################################################

  helper_message("(4/4) Creating review table.", .type = "open")

  drop_cols <- c("review", "member_since", "user_name", "description", "school",
                 "work")

  review[, c(drop_cols) := NULL]

  review[data.table::as.data.table(property),
         c("country", "region", "city") := list(i.country, i.region, i.city),
         on = "property_ID"]

  data.table::setcolorder(
    review, c("review_ID", "date", "user_ID", "user_country", "user_region",
              "user_city"))

  helper_message("(4/4) Review table created.", .type = "close")


  ### RETURN OUTPUT ############################################################

  helper_message("Processing complete.", .type = "final")

  return(lapply(list(review, review_user, review_text, latest_user),
             dplyr::as_tibble))

}
