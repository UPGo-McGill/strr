#' Function to identify STR ghost hostels
#'
#' \code{strr_ghost} takes reported STR listing locations and identifies
#' possible "ghost hostels"--clusters of private-room STR listings operating in
#' a single building.
#'
#' A function for identifying clusters of possible "ghost hostels"--clusters of
#' private-room STR listings operating in a single building. The function works
#' by intersecting the possible locations of listings operated by a single host
#' with each other, to find areas which could the common location of the
#' listings, and thus be one or more housing units subdivided into private rooms
#' rather than a set of geographically disparate listings. The function can
#' optionally run its analysis separately for each date within a time period,
#' and can also check for possible duplication with entire-home listings
#' operated by the same host.
#'
#' @param property A data frame of STR listings with sf or sp point geometries in
#'   a projected coordinate system. If the data frame does not have spatial
#'   attributes, an attempt will be made to convert it to sf using
#'   \code{\link{strr_as_sf}}. The result will be transformed into the Web
#'   Mercator projection (EPSG: 3857) for distance calculations. To use a
#'   projection more suitable to the data, supply an sf or sp object.
#' @param property_ID The name of a character or numeric variable in the property
#'   object which uniquely identifies STR listings.
#' @param host_ID The name of a character or numeric variable in the property
#'   object which uniquely identifies STR hosts.
#' @param multi_date A logical scalar. Should the analysis be run for separate
#'   dates (controlled by the `created`, `scraped`, `start_date` and `end_date`
#'   arguments), or only run a single time, treating all listings as
#'   simultaneously active?
#' @param created The name of a date variable in the property object which gives
#'   the creation date for each listing. This argument is ignored if
#'   `multi_date` is FALSE.
#' @param scraped The name of a date variable in the property object which gives
#'   the last-scraped date for each listing. This argument is ignored if
#'   `multi_date` is FALSE.
#' @param start_date A character string of format YYYY-MM-DD indicating the
#'   first date for which to run the analysis. If NULL (default), all dates will
#'   be used. This argument is ignored if `multi_date` is FALSE.
#' @param end_date A character string of format YYYY-MM-DD indicating the last
#'   date for which to run the analysis. If NULL (default), all dates will be
#'   used. This argument is ignored if `multi_date` is FALSE.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible ghost hostel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @param listing_type The name of a character variable in the property
#'   object which identifies private-room listings. Set this argument to FALSE
#'   to use all listings in the `property` table.
#' @param private_room A character string which identifies the value of the
#'   `listing_type` variable to be used to find ghost hostels. This field is
#'   ignored if `listing_type` is FALSE.
#' @param EH_check A logical scalar. Should ghost hostels be checked against
#'   possible duplicate entire-home listings operated by the same host? This
#'   field is ignored if `listing_type` is FALSE.
#' @param entire_home A character string which identifies the value of the
#'   `listing_type` variable to be used to find possible duplicate entire-home
#'   listings. This field is ignored if `listing_type` or `EH_check` are FALSE.
#' @param geom_type. A character string, either "point" or "polygon", which
#' identifies the type of geometry which should be appended to the function 
#' output. Point geometries will be calculated faster than polygon geometries,
#' and will require less memory.
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be a tidy data frame of identified ghost hostels,
#'   organized with the following fields: `ghost_ID`: an identifier for each
#'   unique ghost hostel cluster. `date`: the date on which the ghost hostel was
#'   detected, if the `created` and `scraped` arguments are supplied. `host_ID`
#'   (or whatever name was passed to the host_ID argument): The ID number of the
#'   host operating the ghost hostel. `listing_count`: how many separate
#'   listings comprised the ghost hostel. `housing_units`: an estimate of how
#'   many housing units the ghost hostel occupies, calculated as
#'   `ceiling(listing_count / 4)`. `property_IDs`: A list of the property_ID
#'   (or whatever name was passed to the property_ID argument) values from the
#'   listings comprising the ghost hostel. `EH_check`: if EH_check is not NULL,
#'   a list of possible entire-home listing duplicates. `data`: a nested tibble
#'   of additional variables present in the property object. `geometry`: the
#'   polygons representing the possible locations of each ghost hostel.
#' @export

strr_ghost <- function(
  property, start_date = NULL, end_date = NULL, property_ID = property_ID,
  host_ID = host_ID, multi_date = TRUE, created = created, scraped = scraped,
  distance = 205, min_listings = 3, listing_type = listing_type,
  private_room = "Private room", EH_check = FALSE,
  entire_home = "Entire home/apt", geom_type = c("point", "polygon"),
  quiet = FALSE) {


  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()
  steps <- 3 + EH_check


  ## Validate arguments --------------------------------------------------------

  helper_check_property(rlang::ensyms(property_ID, host_ID, created, scraped))
  stopifnot(distance > 0, min_listings > 0)
  min_listings <- floor(min_listings)
  geom_type <- geom_type[1]
  stopifnot(geom_type %in% c("point", "polygon"))


  ## Handle spatial attributes -------------------------------------------------

  # Convert property from sp
  if (methods::is(property, "Spatial")) {
    property <- sf::st_as_sf(property)
  }

  # Check that property is sf, and convert to sf if possible
  if (!methods::is(property, "sf")) {
    tryCatch({
      property <- strr_as_sf(property, 3857)
      helper_message("Converting input table to sf.")
    },
    error = function(e) {
      stop(paste0("The object `property` must be of class sf or sp, ",
                  "or must be convertible to sf using strr_as_sf."))
    })
  }

  # Store CRS for later
  crs_property <- sf::st_crs(property)


  ## Set lt_flag and check validity of listing_type ----------------------------

  lt_flag <-
    tryCatch(
      {
        # If listing_type is a field in property, set lt_flag = TRUE
        dplyr::pull(property, {{listing_type}})
        TRUE
      },
      error = function(e) {
        tryCatch(
          {
            # If listing_type == FALSE, set lt_flag = FALSE
            if (!listing_type) { FALSE
            } else stop("`listing_type` must be a valid field name or FALSE.")
          },
          error = function(e2) {
            # Otherwise, fail with an informative error
            stop("`listing_type` must be a valid field name or FALSE.")
          }
        )
      }
    )

  ## Check private_room and entire_home arguments ------------------------------

  if (lt_flag) {

    if (nrow(dplyr::filter(property, {{listing_type}} == private_room)) == 0) {
      warning(paste0("The supplied argument to `private_room` returns no ",
                     "matches in the input table. Are you sure the argument ",
                     "is correct?"))
    }

    if (EH_check &&
        nrow(dplyr::filter(property, {{listing_type}} == entire_home)) == 0) {
      warning(paste0("The supplied argument to `entire_home` returns no ",
                     "matches in the input table. Are you sure the argument ",
                     "is correct?"))
    }
  }

  ## Process dates if multi_date is TRUE ---------------------------------------

  if (multi_date) {

    # Check if created and scraped are dates
    if (!methods::is(dplyr::pull(property, {{created}}), "Date")) {
      tryCatch({
        property <- dplyr::mutate(property, {{created}} := as.Date({{created}}))
      }, error = function(e)
        stop("The `created` field must be coercible to dates.")
      )
    }

    if (!methods::is(dplyr::pull(property, {{scraped}}), "Date")) {
      tryCatch({
        property <- dplyr::mutate(property, {{scraped}} := as.Date({{scraped}}))
      }, error = function(e)
        stop("The `scraped` field must be coercible to dates.")
      )
    }

    # Wrangle start_date/end_date values
    if (missing(start_date)) {
      start_date <- min(dplyr::pull(property, {{created}}), na.rm = TRUE)
    } else {
      start_date <- tryCatch(as.Date(start_date), error = function(e) {
        stop(paste0('The value of `start_date`` ("', start_date,
                    '") is not coercible to a date.'))
      })}

    if (missing(end_date)) {
      end_date <- max(dplyr::pull(property, {{scraped}}), na.rm = TRUE)
    } else {
      end_date <- tryCatch(as.Date(end_date), error = function(e) {
        stop(paste0('The value of `end_date` ("', end_date,
                    '") is not coercible to a date.'))
      })}
  }


  ## Set data.table variables --------------------------------------------------

  data <- date_grid <- starts <- ends <- Var1 <- Var2 <- property_IDs <-
    listing_count <- intersects <- housing_units <- geometry <- ghost_ID <-
    subsets <- start <- end <- subset_list <- NULL

  threads <- data.table::setDTthreads(future::nbrOfWorkers())

  on.exit(data.table::setDTthreads(threads))


  ### PROPERTY SETUP ###########################################################

  helper_message("(1/", steps, ") Identifying possible ghost hostel clusters, ",
                 "using ", helper_plan(), ".", .type = "open")


  ## Prepare table -------------------------------------------------------------

  # Rename fields for easier processing with future and data.table packages
  property <- dplyr::rename(property,
                            property_ID = {{property_ID}},
                            host_ID = {{host_ID}},
                            created = {{created}},
                            scraped = {{scraped}})

  if (lt_flag) property <-
    dplyr::rename(property, listing_type = {{listing_type}})

  # Convert to data.table
  data.table::setDT(property)

  # Remove invalid listings
  property <- property[!is.na(host_ID)]
  
  # Check for multiple host_IDs for one property_ID
  if (nrow(dplyr::filter(
    property, length(unique(host_ID)) > 1, .by = property_ID)) > 0) stop(
      "Multiple host_IDs detected for a single property_ID")


  ## Filter rows ---------------------------------------------------------------

  # Filter to private rooms if lt_flag == TRUE
  if (lt_flag) {
    # Save entire-home listings for later if EH_check == TRUE
    if (EH_check) EH_property <- property[listing_type == entire_home]
    property <- property[listing_type == private_room]
  }

  # Filter property to clusters >= min_listings, and nest by host_ID
  property <-
    property[, if (.N >= min_listings) list(data = list(.SD)), by = "host_ID"]

  # Error handling for case where no clusters are identified
  if (nrow(property) == 0) return(ghost_empty(crs_property))


  ## Identify possible clusters by date if multi_date == TRUE ------------------

  if (multi_date) {

    # Get all start and end dates
    property[, dates := lapply(data, \(x) {
      sort(unique(c(
        c(x[created > start_date]$created, start_date),
        c(x[scraped < end_date]$scraped, end_date) + 1)))
      })]
    
    property[, data := mapply(ghost_date_range, data, dates, 
                              MoreArgs = list(min_listings = min_listings),
                              SIMPLIFY = FALSE)]
    
    # Error handling for one-row case where no clusters were identified
    if (nrow(property) == 1 && is.null(property$data)) 
      return(ghost_empty(crs_property))
    
    # Get rid of rows without valid tables
    property <- property[!sapply(data, is.null)]
    
    # Error handling for case where no clusters are identified
    if (nrow(property) == 0) return(ghost_empty(crs_property))

    # Unnest data, and hold on to dates for later
    property <- property[, unlist(data, recursive = FALSE), by = "host_ID"]

  }
  
  helper_message("(1/", steps, ") Possible ghost hostel clusters identified, ",
                 "using ", helper_plan(), ".", .type = "close")


  ### CLUSTER CREATION AND GHOST HOSTEL IDENTIFICATION #########################

  property_list <- helper_table_split(property, type = "data")

  helper_message("(2/", steps, ") Identifying ghost hostels, using ", 
                 helper_plan(), ".")

  data.table::setDTthreads(1)

  handler_strr("Analyzing row")

  with_progress({

    .strr_env$pb <- progressor(steps = nrow(property))

    property <- par_lapply(property_list, function(x) {
      .strr_env$pb(amount = nrow(x))
      x <- ghost_cluster(x, distance, min_listings)
      x <- ghost_split_clusters(x, distance, min_listings)
      x <- ghost_find_intersect(x, distance, min_listings, geom_type)
      x <- ghost_find_leftovers(x, distance, min_listings, geom_type)
      x
    })

  })

  # Set fill = TRUE to deal with empty outputs
  property <- property[sapply(property, nrow) > 0]
  property <- data.table::rbindlist(property, fill = TRUE)

  data.table::setDTthreads(future::nbrOfWorkers())

  # Error handling for case where no clusters are identified
  if (nrow(property) == 0) return(ghost_empty(crs_property))


  ### GHOST TABLE CREATION #####################################################

  helper_message("(", steps - EH_check, "/", steps,
                 ") Creating final output table, using ", helper_plan(), ".",
                 .type = "open")

  # Remove duplicates now that leftovers have been processed
  property <- property[!duplicated(property$property_IDs),]

  # Make sure the only rows in the nested data are the actual GH points
  property[, data := mapply(function(x, y) x[x$property_ID %in% y,],
                          data, property_IDs, SIMPLIFY = FALSE)]


  ## Create ghost table --------------------------------------------------------

  # Create new fields
  property[, listing_count := lengths(property_IDs)]
  property[, housing_units := as.integer(ceiling(listing_count / 4))]

  # Remove duplicates
  property <- property[!duplicated(property$property_IDs)]

  # Arrange table
  data.table::setkey(property, host_ID)

  # Create ghost_ID and drop extraneous column
  property[, ghost_ID := seq_len(.N)]
  property[, buffers := NULL]

  # Arrange columns
  data.table::setcolorder(property, c("ghost_ID", "host_ID", "listing_count",
                                      "housing_units", "property_IDs", "data"))

  # Convert to sf
  property <- sf::st_as_sf(property)


  ## Calculate dates if multi_date == TRUE -------------------------------------

  if (multi_date) {

    suppressWarnings(data.table::setDT(property))

    # Calculate date ranges
    property[, c("start", "end") := list(
      as.Date(sapply(data,
                     function(x) max(c(start_date, dplyr::pull(x, created)))),
              origin = "1970-01-01"),
      as.Date(sapply(data,
                     function(x) min(c(end_date, dplyr::pull(x, scraped)))),
              origin = "1970-01-01")
    )]

    # Identify subsets
    property[, subsets := lapply(property_IDs, function(y) {
      which(sapply(property$property_IDs, function(x) all(x %in% y)))
    })]

    property[, subsets := mapply(function(x, y) y[y != x], ghost_ID, subsets,
                               SIMPLIFY = FALSE)]

  }

  helper_message("(", steps - EH_check, "/", steps,
                 ") Final output table created, using ", helper_plan(), ".",
                 .type = "close")


  ## EH_check ------------------------------------------------------------------

  if (EH_check) {

    helper_message("(", steps, "/", steps,
                   ") Checking for possible entire-home duplicates.",
                   .type = "open")

    suppressWarnings(data.table::setDT(property))

    EH_buffers <-
      copy(EH_property)[, geometry := sf::st_buffer(geometry, distance)]

    EH_buffers <- sf::st_as_sf(EH_buffers)
    EH_buffers <- sf::st_transform(EH_buffers, crs_property)
    EH_buffers <- dplyr::rename(EH_buffers, EH_property_ID = property_ID)

    gm_fun <- function(x, y) {

      gm <- sf::st_sf(geometry = sf::st_sfc(list(x)), crs = crs_property,
                  agr = "constant")

      EH_host <- sf::st_set_agr(EH_buffers[EH_buffers$host_ID == y,],
                                "constant")

      int <- sf::st_intersection(gm, EH_host)

      dplyr::pull(int, .data$EH_property_ID)
    }

    property[, EH_check := mapply(gm_fun, geometry, host_ID, SIMPLIFY = TRUE)]

    data.table::setcolorder(property,
                            c(setdiff(names(property), "geometry"), "geometry"))

    helper_message("(", steps, "/", steps,
                   ") Possible entire-home duplicates detected.",
                   .type = "close")

  }


  ### TIDY TABLE CREATION ######################################################

  ## Create tidy version of ghost_property -------------------------------------

  if (multi_date) {

    ghost_property <- property[, .(ghost_ID, start, end)]

    ghost_property[, date := mapply(function(x, y) seq(unique(x), unique(y), 1),
                                  start, end, SIMPLIFY = FALSE)]

    # Unnest points by date
    ghost_property <-
      ghost_property[, .(date = as.Date(unlist(date), origin = "1970-01-01")),
                 by = "ghost_ID"]

    # Make sure no invalid dates were introduced
    ghost_property <- ghost_property[date >= start_date & date <= end_date]

    # Rejoin info from property
    ghost_property <- property[ghost_property, on = "ghost_ID"]
    ghost_property[, c("start", "end") := NULL]
    data.table::setcolorder(ghost_property, c("ghost_ID", "date"))

    # Remove rows from ghost_property which are subsets of other rows
    date_subsets <-
      ghost_property[, .(subset_list = list(unlist(subsets))), by = "date"]

    ghost_property <- date_subsets[ghost_property, on = "date"]

    ghost_property <-
      ghost_property[!mapply(function(x, y) x %in% y, ghost_ID, subset_list)]

    ghost_property[, c("subset_list", "subsets") := NULL]

    data.table::setcolorder(ghost_property, c("ghost_ID", "date", "host_ID",
                                              "listing_count", "housing_units",
                                              "property_IDs"))

  } else ghost_property <- property


  ## Convert to sf tibble and rename fields to match input fields --------------

  ghost_property <- dplyr::as_tibble(ghost_property)
  ghost_property <- sf::st_as_sf(ghost_property)
  ghost_property <- dplyr::rename(ghost_property, {{host_ID}} := .data$host_ID)


  ### RETURN OUTPUT ############################################################

  helper_message("Analysis complete.", .type = "final")

  return(ghost_property)

}
