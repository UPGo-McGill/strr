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
#' @param points A data frame of STR listings with sf or sp point geometries in
#'   a projected coordinate system. If the data frame does not have spatial
#'   attributes, an attempt will be made to convert it to sf using
#'   \code{\link{strr_as_sf}}. The result will be transformed into the Web
#'   Mercator projection (EPSG: 3857) for distance calculations. To use a
#'   projection more suitable to the data, supply an sf or sp object.
#' @param property_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR listings.
#' @param host_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR hosts.
#' @param multi_date A logical scalar. Should the analysis be run for separate
#'   dates (controlled by the `created`, `scraped`, `start_date` and `end_date`
#'   arguments), or only run a single time, treating all listings as
#'   simultaneously active?
#' @param created The name of a date variable in the points object which gives
#'   the creation date for each listing. This argument is ignored if
#'   `multi_date` is FALSE.
#' @param scraped The name of a date variable in the points object which gives
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
#' @param listing_type The name of a character variable in the points
#'   object which identifies private-room listings. Set this argument to FALSE
#'   to use all listings in the `points` table.
#' @param private_room A character string which identifies the value of the
#'   `listing_type` variable to be used to find ghost hostels. This field is
#'   ignored if `listing_type` is FALSE.
#' @param EH_check A logical scalar. Should ghost hostels be checked against
#'   possible duplicate entire-home listings operated by the same host? This
#'   field is ignored if `listing_type` is FALSE.
#' @param entire_home A character string which identifies the value of the
#'   `listing_type` variable to be used to find possible duplicate entire-home
#'   listings. This field is ignored if `listing_type` or `EH_check` are FALSE.
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
#'   of additional variables present in the points object. `geometry`: the
#'   polygons representing the possible locations of each ghost hostel.
#' @importFrom data.table setcolorder setkey setDT
#' @importFrom dplyr %>% arrange desc filter group_by mutate n pull rename
#' @importFrom dplyr tibble ungroup
#' @importFrom furrr future_map
#' @importFrom methods is
#' @importFrom purrr map map2 map_dbl map_int map_lgl
#' @importFrom rlang .data
#' @importFrom sf st_as_sf st_as_sfc st_crs st_point st_sf st_transform
#' @importFrom tidyr nest unnest
#' @export

strr_ghost <- function(
  points, start_date = NULL, end_date = NULL, property_ID = property_ID,
  host_ID = host_ID, multi_date = TRUE, created = created, scraped = scraped,
  distance = 200, min_listings = 3, listing_type = listing_type,
  private_room = "Private room", EH_check = FALSE,
  entire_home = "Entire home/apt", quiet = FALSE) {


  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  time_1 <- Sys.time()

  helper_progress_message("Analyzing ghost hostels.")

  steps <- 3 + multi_date + EH_check


  ## Set data.table and future variables

  .datatable.aware = TRUE

  data <- date_grid <- starts <- ends <- Var1 <- Var2 <- property_IDs <-
    listing_count <- intersects <- housing_units <- geometry <- ghost_ID <-
    subsets <- start <- end <- subset_list <- NULL

  threads <- setDTthreads(future::nbrOfWorkers())

  options(future.globals.maxSize = +Inf)


  ## Check distance, min_listings flags

  # Check that distance > 0
  if (distance <= 0) {
    stop("The argument `distance` must be a positive number.")
  }

  # Check that min_listings is an integer > 0
  min_listings <- floor(min_listings)
  if (min_listings <= 0) {
    stop("The argument `min_listings` must be a positive integer.")
  }


  ## Handle spatial attributes

  # Convert points from sp
  if (is(points, "Spatial")) {
    points <- st_as_sf(points)
  }

  # Check that points is sf, and convert to sf if possible
  if (!is(points, "sf")) {
    tryCatch({
      points <- strr_as_sf(points, 3857)
      helper_progress_message("Converting input table to sf.")
    },
    error = function(e) {
      stop(paste0("The object `points` must be of class sf or sp, ",
                  "or must be convertable to sf using strr_as_sf."))
    })
  }

  # Store CRS for later
  crs_points <- st_crs(points)


  ## Check that points fields exist

  tryCatch(
    pull(points, {{ property_ID }}),
    error = function(e) {
      stop("The value of `property_ID` is not a valid field in the input table."
      )})

  tryCatch(
    pull(points, {{ host_ID }}),
    error = function(e) {
      stop("The value of `host_ID` is not a valid field in the input table."
      )})

  tryCatch(
    pull(points, {{ created }}),
    error = function(e) {
      stop("The value of `created` is not a valid field in the input table."
      )})

  tryCatch(
    pull(points, {{ scraped }}),
    error = function(e) {
      stop("The value of `scraped` is not a valid field in the input table."
      )})


  ## Set lt_flag and check validity of listing_type

  lt_flag <-
    tryCatch(
      {
        # If listing_type is a field in points, set lt_flag = TRUE
        pull(points, {{ listing_type }})
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

  ## Check private_room and entire_home arguments

  if (lt_flag) {

    if (points %>% filter({{ listing_type }} == private_room) %>% nrow() == 0) {
      warning(paste0("The supplied argument to `private_room` returns no ",
                     "matches in the input table. Are you sure the argument ",
                     "is correct?"))
    }

    if (EH_check &&
        points %>% filter({{ listing_type }} == entire_home) %>% nrow() == 0) {
      warning(paste0("The supplied argument to `entire_home` returns no ",
                     "matches in the input table. Are you sure the argument ",
                     "is correct?"))
    }
  }

  ## Process dates if multi_date is TRUE

  if (multi_date) {

    # Check if created and scraped are dates
    if (!is(pull(points, {{ created }}), "Date")) {
      stop("The `created` field must be of class 'Date'")
    }

    if (!is(pull(points, {{ scraped }}), "Date")) {
      stop("The `scraped` field must be of class 'Date'")
    }

    # Wrangle start_date/end_date values
    if (missing(start_date)) {
      start_date <-
        points %>%
        pull({{ created }}) %>%
        min(na.rm = TRUE)
    } else {
      start_date <- tryCatch(as.Date(start_date), error = function(e) {
        stop(paste0('The value of `start_date`` ("', start_date,
                    '") is not coercible to a date.'))
      })}

    if (missing(end_date)) {
      end_date <-
        points %>%
        pull({{ scraped }}) %>%
        max(na.rm = TRUE)
    } else {
      end_date <- tryCatch(as.Date(end_date), error = function(e) {
        stop(paste0('The value of `end_date` ("', end_date,
                    '") is not coercible to a date.'))
      })}
  }


  ### POINTS SETUP #############################################################

  helper_progress_message("(1/", steps,
                          ") Identifying possible ghost hostel clusters, ",
                          "using {helper_plan()}.",
                          .type = "open")

  # Rename fields for easier processing with future and data.table packages
  points <-
    points %>%
    rename(property_ID = {{ property_ID }},
           host_ID = {{ host_ID }},
           created = {{ created}},
           scraped = {{ scraped }})

  if (lt_flag) points <- points %>% rename(listing_type = {{ listing_type }})

  # Convert to data.table
  setDT(points)

  # Remove invalid listings
  points <- points[!is.na(host_ID)]

  # Filter to private rooms if listing_type != FALSE
  if (lt_flag) {

    # Save entire-home listings for later if EH_check == TRUE
    if (EH_check) EH_points <- points[listing_type == entire_home]

    points <- points[listing_type == private_room]
  }

  # Filter points to clusters >= min_listings, and nest by host_ID
  points <-
    points[, if (.N >= min_listings) list(data = list(.SD)), by = "host_ID"]


  # Error handling for case where no clusters are identified
  if (nrow(points) == 0) return(ghost_empty(crs_points))

  # Identify possible clusters by date if multi_date == TRUE
  if (multi_date) {

    points[, c("starts", "ends") := list(
      lapply(data, function(x) {
        # Add start_date to account for listings with created < start_date
        c(unique(x[created > start_date]$created), start_date)}),
      lapply(data, function(x) {
        # Add end_date to account for listings with scraped > end_date
        c(unique(x[scraped < end_date]$scraped), end_date)}))]

    # Make list of all combinations of starts and ends
    points[, date_grid := mapply(expand.grid, starts, ends, SIMPLIFY = FALSE)]
    points[, date_grid := lapply(date_grid, function(x) setDT(x)[Var1 <= Var2])]

    # Error handling for case where no clusters are identified
    if (nrow(points) == 0) return(ghost_empty(crs_points))

    helper_progress_message("(1/", steps,
                            ") Possible ghost hostel clusters identified, ",
                            "using {helper_plan()}.",
                            .type = "close")

    # Create a nested tibble for each possible cluster
    helper_progress_message("(2/", steps,
                            ") Preparing possible clusters for analysis, ",
                            "using {helper_plan()}.",
                            .type = "open")

    apply_fun <-
      function(x, y) {
        output <- apply(x, 1, function(z) y[created <= z[1] & scraped >= z[2]])
        output <- unique(output)
        output[lapply(output, nrow) >= min_listings]
      }

    points[, data := mapply(apply_fun, date_grid, data, SIMPLIFY = FALSE)]
    points[, c("starts", "ends", "date_grid") := list(NULL, NULL, NULL)]
    points <-
      points[, .(data = unlist(data, recursive = FALSE)), by = "host_ID"]

    helper_progress_message("(2/", steps,
                            ") Possible clusters prepared for analysis, ",
                            "using {helper_plan()}.",
                            .type = "close")
  } else {
    helper_progress_message("(1/", steps,
                            ") Possible ghost hostel clusters identified, ",
                            "using {helper_plan()}.",
                            .type = "close")
  }


  ### CLUSTER CREATION AND GHOST HOSTEL IDENTIFICATION #########################

  points_list <- points %>% helper_table_split(type = "data")

  helper_progress_message("(", 2 + multi_date, "/", steps,
                          ") Identifying ghost hostels, using {helper_plan()}.",
                          .type = "progress")

  setDTthreads(1)

  points <-
    points_list %>%
    furrr::future_map(~{
      .x %>%
        ghost_cluster(distance, min_listings) %>%
        ghost_intersect(distance, min_listings) %>%
        ghost_intersect_leftovers(distance, min_listings)
    },
    # Suppress progress bar if !quiet or the plan is remote
    .progress = helper_progress()
    ) %>%
    rbindlist()

  setDTthreads(future::nbrOfWorkers())

  # Error handling for case where no clusters are identified
  if (nrow(points) == 0) return(ghost_empty(crs_points))


  ### GHOST TABLE CREATION #####################################################

  helper_progress_message("(", steps - EH_check, "/", steps,
                          ") Creating final output table, ",
                          "using {helper_plan()}.",
                          .type = "open")

  # Remove duplicates now that leftovers have been processed
  points <- points[!duplicated(points$property_IDs),]

  # Make sure the only rows in the nested data are the actual GH points
  points[, data := mapply(function(x, y) x[property_ID %in% y],
                          data, property_IDs, SIMPLIFY = FALSE)]

  # Generate geometry column for ghost table
  ghost_geom <-
    points$intersects %>%
    map(~{.x$geometry}) %>%
    do.call(rbind, .) %>%
    st_as_sfc()


  ## Create ghost table

  # Create new fields and join geometry
  points[, listing_count := sapply(intersects, function(x) x$n.overlaps)]
  points[, housing_units := as.integer(ceiling(listing_count / 4))]
  points[, geometry := ghost_geom]

  # Remove duplicates
  points <- points[!duplicated(points$property_IDs)]

  # Arrange table
  setkey(points, host_ID)

  # Create ghost_ID
  points[, ghost_ID := seq_len(.N)]

  # Arrange columns
  points <-
    points[, .(ghost_ID, host_ID, listing_count, housing_units, property_IDs,
               data, geometry)]

  # Convert to sf and reattach CRS
  points <- st_as_sf(points, crs = crs_points)


  ## Calculate dates if multi_date == TRUE

  if (multi_date) {

    setDT(points)

    # Calculate date ranges
    points[, c("start", "end") := list(
      as.Date(sapply(data, function(x) max(c(start_date, pull(x, created)))),
              origin = "1970-01-01"),
      as.Date(sapply(data, function(x) min(c(end_date,   pull(x, scraped)))),
              origin = "1970-01-01")
    )]

    # Identify subsets

    points[, subsets := lapply(property_IDs, function(y) {
      which(map_lgl(points$property_IDs, ~all(.x %in% y)))
    })]

    points[, subsets := mapply(function(x, y) y[y != x], ghost_ID, subsets,
                               SIMPLIFY = FALSE)]

  }

  helper_progress_message("(", steps - EH_check, "/", steps,
                          ") Final output table created, ",
                          "using {helper_plan()}.",
                          .type = "close")


  ## EH_check

  if (EH_check) {

    helper_progress_message("(", steps, "/", steps,
                            ") Checking for possible entire-home duplicates.",
                            .type = "open")

    setDT(points)

    EH_buffers <-
      copy(EH_points)[, geometry := st_buffer(geometry, distance)] %>%
      st_as_sf() %>%
      st_transform(crs_points) %>%
      rename(EH_property_ID = property_ID)

    EH_gm_fun <- function(x, y) {
      gm <- st_sf(geometry = st_sfc(list(x)), crs = crs_points,
                  agr = "constant")
      EH_host <- st_set_agr(EH_buffers[EH_buffers$host_ID == y,], "constant")
      st_intersection(gm, EH_host) %>% pull(.data$EH_property_ID)
    }

    points[, EH_check := mapply(EH_gm_fun, geometry, host_ID, SIMPLIFY = TRUE)]

    setcolorder(points, c(setdiff(names(points), "geometry"), "geometry"))

    helper_progress_message("(", steps, "/", steps,
                            ") Possible entire-home duplicates detected.",
                            .type = "close")

  }


  ### TIDY TABLE CREATION ######################################################

  # Create tidy version of ghost_points

  if (multi_date) {

    ghost_points <- points[, .(ghost_ID, start, end)]

    ghost_points[, date := mapply(function(x, y) seq(unique(x), unique(y), 1),
                                  start, end, SIMPLIFY = FALSE)]

    # Unnest points by date
    ghost_points <-
      ghost_points[, .(date = as.Date(unlist(date), origin = "1970-01-01")),
                 by = "ghost_ID"]

    # Make sure no invalid dates were introduced
    ghost_points <- ghost_points[date >= start_date & date <= end_date]

    # Rejoin info from points
    ghost_points <- points[ghost_points, on = "ghost_ID"]
    ghost_points[, c("start", "end") := NULL]
    setcolorder(ghost_points, c("ghost_ID", "date"))

    # Remove rows from ghost_points which are subsets of other rows

    date_subsets <-
      ghost_points[, .(subset_list = list(unlist(subsets))), by = "date"]

    ghost_points <- date_subsets[ghost_points, on = "date"]

    ghost_points <-
      ghost_points[!mapply(function(x, y) x %in% y, ghost_ID, subset_list)]

    ghost_points[, c("subset_list", "subsets") := NULL]

    setcolorder(ghost_points, c("ghost_ID", "date", "host_ID", "listing_count",
                                "housing_units", "property_IDs"))

  } else ghost_points <- points

  # Convert to sf tibble and rename fields to match input fields
  ghost_points <-
    ghost_points %>%
    as_tibble() %>%
    st_as_sf() %>%
    rename({{ host_ID }} := .data$host_ID)


  ### RETURN OUTPUT ############################################################

  helper_progress_message("Analysis complete.", .type = "final")

  return(ghost_points)
}



#' Helper function to create potential ghost hostel clusters
#'
#' \code{ghost_cluster} takes `points` and splits it into potential clusters.
#'
#' A function for splitting `points` into clusters of potential ghost hostels,
#' with each cluster having length >= min_listings.
#'
#' @param points A data frame of STR listings with sf or sp point geometries in
#'   a projected coordinate system.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible ghost hostel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be the `points` object, rearranged with one row per
#'   cluster and with a new `predicates` field.
#' @importFrom dplyr %>% filter mutate
#' @importFrom purrr map map2 reduce
#' @importFrom rlang .data
#' @importFrom sf st_buffer st_intersects
#' @importFrom tidyr unnest

ghost_cluster <- function(points, distance, min_listings) {

  predicates <- data <- NULL

  # Create intersect predicate lists
  points[, predicates := lapply(data, function(x) {
    st_intersects(st_buffer(st_as_sf(x), distance))
  })]

  # Extra list around lapply is needed to force embedded list where nrow == 1
  points[, predicates := list(lapply(predicates, function(pred) {
    map(seq_along(pred), ~{
      # Merge lists with common elements
      reduce(pred, function(x, y) if (any(y %in% x)) unique(c(x, y)) else x,
      .init = pred[[.]]) # Compile lists starting at each position
    }) %>%
      map(sort) %>%
      unique() # Remove duplicate lists
  }))]

  # Remove lists < min_listings
  points[, predicates := list(lapply(predicates,
                                function(x) x[lengths(x) >= min_listings]))]

  # Use predicates to split points into clusters with length >= min_listings
  points <- points[lapply(predicates, length) > 0]

  pred_fun <- function(x, y) map(y, ~x[.,])

  points[, data := list(mapply(pred_fun, data, predicates, SIMPLIFY = FALSE))]

  # Exit function early if points table is empty
  if (nrow(points) == 0) {
    return(data.table(host_ID = character(), data = list()))
  }

  # Unnest data
  points <- points[, .(data = unlist(data, recursive = FALSE)), by = "host_ID"]

  # Remove duplicate clusters
  points <- points[!duplicated(points$data),]

  points
}


#' Helper function to create potential intersection combinations
#'
#' \code{ghost_combine} takes `buffers` and `predicates` and generates
#' combinations.
#'
#' A function for generating potential intersection combinations using `buffers`
#' and `predicates` from points$data.
#'
#' @param buffers A data frame of buffers from points$data.
#' @param predicates The points$data field generated from \code{st_intersects}.
#' @param n A numeric scalar. The number of points to attempt to find a set of
#'   combinations for
#' @return The output will be a matrix of possible intersection combinations.
#' @importFrom dplyr %>% as_tibble
#' @importFrom purrr map map_dbl
#' @importFrom rlang .data
#' @importFrom sf st_centroid st_distance st_union
#' @importFrom utils combn

ghost_combine <- function(buffers, predicates, n) {

  # Get valid buffers and predicates for combinations
  valid_pr <- predicates
  valid_pr2 <- valid_pr
  invalid_pr <- which(tabulate(unlist(valid_pr)) < n)
  if (length(invalid_pr) > 0) {
    valid_pr <- map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
  }

  while (!identical(valid_pr, valid_pr2)) {
    valid_pr2 <- valid_pr
    invalid_pr <- unique(c(invalid_pr, which(tabulate(unlist(valid_pr)) < n)))
    valid_pr <- map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
    if (length(valid_pr) < n) break
  }

  # Test if any valid combinations could exist, and exit if not
  if (length(valid_pr) < n) return(matrix(nrow = 0, ncol = 0))

  # Identify sets to generate combinations from
  combinations <- map(valid_pr, function(x){
    x[map(valid_pr, ~which(x %in% .)) %>%
        unlist() %>%
        tabulate() >= n]
  }) %>%
    unique()

  # Test if reducer will be necessary to avoid too many combinations
  while(sum(map_dbl(combinations, ~{
    factorial(length(.)) / {factorial(n) * factorial(length(.) - n)}
  })) > 100000) {

    # Establish collective centroid
    if (length(invalid_pr) > 0) {
      valid_bf <- buffers[-invalid_pr,]
    } else {valid_bf <- buffers}

    centroid <-
      valid_bf %>%
      st_centroid() %>%
      st_union() %>%
      st_centroid()

    # Identify furthest point
    to_remove <-
      which(buffers$property_ID == valid_bf[which.max(st_distance(
        st_centroid(valid_bf), centroid)),]$property_ID)

    # Remove furthest point from predicate list
    invalid_pr <- c(invalid_pr, to_remove)

    # Repeat previous steps to generate new combinations
    valid_pr <- map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
    while (!identical(valid_pr, valid_pr2)) {
      valid_pr2 <- valid_pr
      invalid_pr <- unique(c(invalid_pr, which(tabulate(unlist(valid_pr)) < n)))
      valid_pr <- map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
      if (length(valid_pr) < n) break
    }
    if (length(valid_pr) < n) return(matrix(nrow = 0, ncol = 0))
    combinations <- map(valid_pr, function(x){
      x[map(valid_pr, ~which(x %in% .)) %>%
          unlist() %>%
          tabulate() >= n]
    }) %>%
      unique()
  }

  # Generate combinations from valid buffers and discard duplicates
  combinations <-
    map(combinations, combn, n) %>%
    do.call(cbind, .) %>%
    unique(MARGIN = 2)

  # Reduce combinations to valid options
  combinations <- combinations[, apply(combinations, 2, function(x) {
    sapply(valid_pr, function(y) all(x %in% y))
  }) %>%
    colSums() >= n]

  # Convert combinations matrix to tibble for future steps
  combinations <- suppressWarnings(as_tibble(combinations))

  combinations
}


#' Helper function to intersect two buffers
#'
#' \code{ghost_intersect_with_done} takes the intersection of two buffers.
#'
#' A function for intersecting two buffers with done(), for use in reduce().
#'
#' @param x A buffer.
#' @param y A buffer.
#' @return The output will be an intersect polygon.
#' @importFrom rlang .data done
#' @importFrom sf st_intersection

ghost_intersect_with_done <- function(x, y) {
  result <- st_intersection(x, y)
  if (nrow(result) == 0) done(result) else result
}


#' Helper function to intersect a data frame of buffers
#'
#' \code{ghost_stepwise_intersect} applies st_intersection in stepwise fashion
#' to a data frame of buffers.
#'
#' A function for intersecting a data frame of buffers in stepwise fashion,
#' including a number of optimizations for performance.
#'
#' @param buffers A data frame of buffers from points$data.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be an intersect polygon.
#' @importFrom data.table setDF
#' @importFrom dplyr %>% as_tibble distinct mutate
#' @importFrom purrr map reduce
#' @importFrom rlang .data
#' @importFrom sf st_agr<- st_as_sf st_intersects st_is

ghost_stepwise_intersect <- function(buffers, min_listings) {

  # Deal with 0-length input
  if (nrow(buffers) == 0) return(buffers)

  # Build predicates and h_score
  predicates <- st_intersects(buffers)
  matrix <-
    matrix(c(sort(lengths(predicates), decreasing = TRUE),
             seq_along(predicates)),
           nrow = length(predicates))
  h_score <- max(matrix[matrix[,2] >= matrix[,1],1])

  # Set up variables
  st_agr(buffers) <- "constant"
  n <- h_score # Get working number of rows for combinations
  cols <- ncol(buffers) - 1 # Get total columns for clean-up

  # Generate combinations from predicates
  combinations <- ghost_combine(buffers, predicates, n)

  # Ensure valid combination list
  while (ncol(combinations) == 0 && n >= min_listings) {
    n <- n - 1
    combinations <- ghost_combine(buffers, predicates, n)
  }

  # Exit function if no valid combinations exist
  if (n < min_listings) {
    return(
      buffers[0,] %>% mutate(n.overlaps = as.integer(NA), origin = list(c(0)))
    )
  }

  # Master while-loop
  while (n >= min_listings) {

    # Try all combinations for a given n
    intersect_output <-
      map(combinations, function(x, n) {
        intersect <- suppressWarnings(
          split(st_as_sf(setDF(buffers[x,])), seq_len(nrow(buffers[x,]))) %>%
            reduce(ghost_intersect_with_done))
        intersect <- intersect[, 1:cols]
        mutate(intersect, n.overlaps = n, origins = list(x))
      }, n = n)

    # Discard null results and rbind to single sf tibble
    intersect_output <-
      intersect_output[map(intersect_output, nrow) > 0] %>%
      do.call(rbind, .) %>%
      as_tibble()

    # Conditional to decide if the while-loop should continue
    if (nrow(intersect_output) == 0) {
      n <- n - 1
      combinations <- ghost_combine(buffers, predicates, n)
    } else {
      intersect_output <-
        intersect_output %>%
        st_as_sf() %>%
        distinct(.data$geometry, .keep_all = TRUE)

      if (any(st_is(intersect_output, "POLYGON") == FALSE)) {
        stop("Invalid geometry produced.")
      }

      return(intersect_output)
    }
  }

  intersect_output
}


#' Helper function to find ghost hostel locations
#'
#' \code{ghost_intersect} finds ghost hostel locations from a set of clusters.
#'
#' A function for finding ghost hostel locations from a set of point clusters.
#'
#' @param points An sf data frame of STR listings nested by cluster.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible ghost hostel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be `points`, trimmed to valid ghost hostel locations
#'   and with additional fields added.
#' @importFrom dplyr %>% filter mutate
#' @importFrom purrr map map2
#' @importFrom rlang .data
#' @importFrom sf st_area st_buffer

ghost_intersect <- function(points, distance, min_listings) {

  buffers <- data <- intersects <- property_IDs <- NULL

  # Prepare buffers
  points[, buffers := lapply(data, function(x) st_buffer(st_as_sf(x),
                                                         dist = distance))]

  # Create intersects using ghost_stepwise_intersect
  points[, intersects := map(buffers, ghost_stepwise_intersect,
                             min_listings)]

  # Remove empty clusters
  points <- points[lapply(intersects, nrow) > 0]

  # Choose intersect with max area
  points[lapply(intersects, nrow) > 1,
         intersects := lapply(intersects,
                              function(x) x[which.max(st_area(x)),])]

  # Add $property_IDs field
  data_PIDs <- map(points$data, `$`, "property_ID")
  int_origs <- map(points$intersects, `$`, "origins")

  points[, property_IDs := mapply(function(x, y) y[x[[1]]], int_origs,
                                  data_PIDs, SIMPLIFY = FALSE)]

  points
}


#' Helper function to identify leftover ghost hostel candidates
#'
#' \code{ghost_identify_leftovers} identifies leftover ghost hostel candidates.
#'
#' A function for identifying leftover ghost hostel candidates after the main
#' ghost hostel analysis has been run.
#'
#' @param points An sf data frame of STR listings nested by cluster.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be a set of new candidate `points`.
#' @importFrom dplyr %>% filter mutate select
#' @importFrom purrr map_int map2
#' @importFrom rlang .data

ghost_identify_leftovers <- function(points, min_listings) {

  data <- property_ID <- property_IDs <- host_ID <- NULL

  leftovers <-
    points[sapply(data, nrow) - lengths(property_IDs) >= min_listings]

  leftovers[, data := mapply(function(x, y) x[!property_ID %in% y], data,
                             property_IDs, SIMPLIFY = FALSE)]

  leftovers <- leftovers[, .(host_ID, data)]
  leftovers <- leftovers[!duplicated(leftovers$data)]

  leftovers
}


#' Helper function to rerun analysis on leftover ghost hostel candidates
#'
#' \code{ghost_intersect_leftovers} reruns the ghost hostel analysis on leftover
#'  candidates.
#'
#' A function for rerunning the ghost hostel analysis on leftover candidates
#' after the main analysis has been run.
#'
#' @param points An sf data frame of STR listings nested by cluster.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible ghost hostel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be the `points` file with additional ghost hostels
#'   added.
#' @importFrom dplyr %>% filter mutate
#' @importFrom purrr map_int map2
#' @importFrom rlang .data

ghost_intersect_leftovers <- function(points, distance, min_listings) {

  data <- property_IDs <- property_ID <- NULL

  if (nrow(points) == 0) return(points)

  # Subset leftover candidates
  leftovers <- ghost_identify_leftovers(points, min_listings)

  # Condition to run ghost_intersect on leftovers
  while (nrow(leftovers) > 0) {

    # Remove leftovers from points$data
    points <-
      rbind(
        points[sapply(data, nrow) - lengths(property_IDs) < min_listings],
        # Add leftovers in case there is only a partial match
        points[sapply(data, nrow) - lengths(property_IDs) >= min_listings
               ][, data := mapply(function(x, y) x[property_ID %in% y], data,
                                  property_IDs, SIMPLIFY = FALSE)])

    # Apply ghost_intersect to leftovers
    leftover_outcome <- ghost_intersect(leftovers, distance, min_listings)

    # Add leftover_outcome to points
    points <- rbind(points, leftover_outcome)

    # Subset leftover candidates again
    leftovers <- ghost_identify_leftovers(points, min_listings)
  }

  points
}


#' Helper function to output an empty ghost hostel table
#'
#' \code{ghost_empty} produces an empty output table.
#'
#' A function for producing an empty output table if there are no valid ghost
#' hostels.
#'
#' @param crs_points The CRS of the points table.
#' @importFrom dplyr %>% mutate tibble
#' @importFrom sf st_as_sf st_sfc
#' @importFrom rlang .data

ghost_empty <- function(crs_points) {
  tibble(ghost_ID = integer(),
         date = as.Date(x = integer(), origin = "1970-01-01"),
         host_ID = character(),
         listing_count = integer(),
         housing_units = integer(),
         property_IDs = list(),
         data = list(),
         geometry = st_sfc()) %>%
    st_as_sf(crs = crs_points)
}
