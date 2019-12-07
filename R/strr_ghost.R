#' Function to identify STR ghost hostels
#'
#' \code{strr_ghost} takes reported STR listing locations and identifies
#' possible "ghost hostels"--clusters of private-room STR listings operating in
#' a single building.
#'
#' A function for probablistically assigning STR listings to administrative
#' geographies (e.g. census tracts) based on reported latitude/longitude.
#' The function works by combining a known probability density function (e.g.
#' Airbnb's spatial obfuscation of listing locations) with an additional source
#' of information about possible listing locations--either population or housing
#' densities.
#'
#' @param points A data frame of STR listings with sf or sp point geometries in
#'   a projected coordinate system.
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
#' @param cores A positive integer scalar. How many processing cores should be
#'   used to perform the computationally intensive intersection steps? The
#'   implementation of multicore processing does not support Windows, so this
#'   argument should be left with its default value of 1 in those cases.
#' @param quiet A logical vector. Should the function execute quietly, or should
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
#' @importFrom dplyr %>% arrange as_tibble enquo filter group_by mutate n pull
#' @importFrom dplyr quo_name rename ungroup
#' @importFrom methods is
#' @importFrom purrr map map2 map_dbl map_lgl
#' @importFrom rlang .data
#' @importFrom sf st_as_sf st_crs st_crs<- st_point st_transform
#' @export

strr_ghost <- function(
  points, property_ID = property_ID, host_ID = host_ID, multi_date = TRUE,
  created = created, scraped = scraped, start_date = NULL, end_date = NULL,
  distance = 200, min_listings = 3, listing_type = listing_type,
  private_room = "Private room", EH_check = FALSE,
  entire_home = "Entire home/apt", cores = 1, quiet = FALSE) {



  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  time_1 <- Sys.time()

  ## Cores, distance, min_listings

  # Check that cores is an integer > 0
  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }

  # Check that distance > 0
  if (distance <= 0) {
    stop("The argument `distance` must be a positive number.")
  }

  # Check that min_listings is an integer > 0
  min_listings <- floor(min_listings)
  if (min_listings <= 0) {
    stop("The argument `min_listings` must be a positive integer.")
  }


  ## Points table

  # Convert points from sp
  if (is(points, "Spatial")) {
    points <- st_as_sf(points)
  }

  # Check that points is sf
  if (!is(points, "sf")) {
    stop("The object `points` must be of class sf or sp.")
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

  if (!quiet) message("Filtering listings to ghost hostel candidates. (",
                      substr(Sys.time(), 12, 19), ")")

  # Remove invalid listings
  points <-
    points %>%
    filter(!is.na({{ host_ID }}))

  # Filter to private rooms if listing_type != FALSE
  if (lt_flag) {

    # Save entire-home listings for later if EH_check == TRUE
    if (EH_check) {
      EH_points <- filter(points, {{ listing_type }} == entire_home)
    }

    points <-
      points %>%
      filter({{ listing_type }} == private_room)
  }

  # Filter points to clusters >= min_listings, and nest by host_ID
  points <-
    points %>%
    group_by({{ host_ID }}) %>%
    filter(n() >= min_listings) %>%
    tidyr::nest() %>%
    ungroup()

  # Error handling for case where no clusters are identified
  if (nrow(points) == 0) return(ghost_empty(points, crs_points))

  # Identify possible clusters by date if multi_date == TRUE
  if (multi_date) {

    if (!quiet) message("Identifying possible ghost hostel clusters by date. (",
                        substr(Sys.time(), 12, 19), ")")

    points <-
      points %>%
      mutate(
        starts = map(.data$data, ~{
          filter(.x, {{ created }} > start_date) %>%
            pull({{ created }}) %>%
            unique() %>%
            # Add start_date to account for listings with created < start_date
            c(start_date)
        }),
        ends = map(.data$data, ~{
          filter(.x, {{ scraped }} < end_date) %>%
            pull({{ scraped }}) %>%
            unique() %>%
            # Add end_date to account for listings with scraped > end_date
            c(end_date)
        }),
        # Make list of all combinations of starts and ends
        date_grid = map2(.data$starts, .data$ends, expand.grid),
        date_grid = map(.data$date_grid, filter, .data$Var1 <= .data$Var2)
      )

    # Error handling for case where no clusters are identified
    if (nrow(points) == 0) return(ghost_empty(points, crs_points))

    # Create a nested tibble for each possible cluster

    if (!quiet) message("Preparing possible clusters for analysis. (",
                        substr(Sys.time(), 12, 19), ")")

    points <-
      points %>%
      mutate(data = map2(.data$date_grid, .data$data, function(x, y) {
        apply(x, 1, function(z) {
          filter(y, {{ created }} <= z[1], {{ scraped }} >= z[2])
        }) %>%
          unique() %>%
          `[`(lapply(., nrow) >= min_listings)
      })) %>%
      tidyr::unnest(.data$data) %>%
      select(-.data$starts, -.data$ends, -.data$date_grid)
  }



  ### CLUSTER CREATION AND GHOST HOSTEL IDENTIFICATION #########################

  # Multi-threaded version
  if (cores >= 2) {

    if (!quiet) message("Splitting clusters for multicore processing. (",
                        substr(Sys.time(), 12, 19), ")")

    clusters <- pbapply::splitpb(nrow(points), cores, nout = 100)
    points_list <- lapply(clusters, function(x) points[x,])
    cl <- parallel::makeForkCluster(cores)

    if (!quiet) message("Identifying ghost hostels. (",
                        substr(Sys.time(), 12, 19), ")")

    points <-
      points_list %>%
      pbapply::pblapply(function(x) {
        x %>%
          ghost_cluster(distance, min_listings) %>%
          ghost_intersect({{ property_ID }}, distance, min_listings) %>%
          ghost_intersect_leftovers({{ property_ID }}, {{ host_ID }}, distance,
                                    min_listings)
      }, cl = cl) %>%
      do.call(rbind, .)

  } else {
    # Single-threaded version

    if (!quiet) message("Splitting clusters for ghost hostel identification. (",
                        substr(Sys.time(), 12, 19), ")")
    points <- ghost_cluster(points, distance, min_listings)

    if (!quiet) message("Identifying ghost hostels. (",
                        substr(Sys.time(), 12, 19), ")")
    points <- ghost_intersect(points, {{ property_ID }}, distance,
                              min_listings)

    if (!quiet) message("Rerunning analysis on leftover points. (",
                        substr(Sys.time(), 12, 19), ")")
    points <- ghost_intersect_leftovers(points, {{ property_ID }},
                                        {{ host_ID }}, distance, min_listings)
  }

  # Error handling for case where no clusters are identified
  if (nrow(points) == 0) return(ghost_empty(points, crs_points))



  ### GHOST TABLE CREATION #####################################################

  if (!quiet) message("Combining ghost hostels into output table. (",
                      substr(Sys.time(), 12, 19), ")")

  points <-
    points %>%
    mutate(data = map2(.data$data, .data$property_IDs, ~{
      filter(.x, {{ property_ID }} %in% .y)}))

  # Generate compact table of ghost hostels, suppressing sf geometry warnings
  # THE NEW UNNEST SEEMS TO HAVE AN SF FAILURE TKTK
  points <- suppressWarnings(
    tidyr::unnest_legacy(points, .data$intersects, .preserve = .data$data)
  )

  # Remove duplicates
  points <- points[!duplicated(points$property_IDs),]

  # Create Ghost_ID
  points <-
    points %>%
    arrange( {{ host_ID }}) %>%
    mutate(ghost_ID = 1:n())

  # Extract geometry, coerce to sf, join back to ghost_points, clean up
  ### THIS IS STEP THAT GENERATES OGR UNSUPPORTED TKTK
  points <-
    points[c("ghost_ID","geometry")] %>%
    st_as_sf() %>%
    left_join(st_drop_geometry(st_as_sf(points)), ., by = "ghost_ID") %>%
    st_as_sf() %>%
    rename(listing_count = .data$n.overlaps) %>%
    mutate(housing_units = as.integer(ceiling(.data$listing_count / 4))) %>%
    select(.data$ghost_ID, {{ host_ID }}, .data$listing_count,
           .data$housing_units, .data$property_IDs, .data$data, .data$geometry)

  # Reattach CRS
  st_crs(points) <- crs_points

  # Calculate dates if multi_date == TRUE
  if (multi_date) {

    if (!quiet) message("Identifying active date ranges for ghost hostels. (",
                        substr(Sys.time(), 12, 19), ")")

    # Calculate date ranges
    points <-
      points %>%
      mutate(
        start = map_dbl(.data$data, ~{
          pull(.x, {{ created }}) %>%
            max(c(., start_date))
        }) %>%
          as.Date(origin = "1970-01-01"),
        end = map_dbl(.data$data, ~{
          pull(.x, {{ scraped }}) %>%
            min(c(., end_date))
        }) %>%
          as.Date(origin="1970-01-01")
      )

    # Identify subsets
    points <-
      points %>%
      mutate(
        subsets = map(.data$property_IDs, function(y) {
          which(map_lgl(points$property_IDs, ~all(.x %in% y)))
        }),
        subsets = map2(.data$ghost_ID, .data$subsets, ~{.y[.y != .x]}))
  }

  ## EH_check

  if (!missing(EH_check)) {

    EH_buffers <-
      st_buffer(EH_points, distance) %>%
      st_transform(crs_points) %>%
      rename(EH_property_ID = {{ property_ID }})

    points <-
      points %>%
      mutate(EH_check = map2(.data$geometry, {{ host_ID }}, ~{

        geom <-
          tibble::tibble(geometry = list(.x)) %>%
          st_as_sf(crs = crs_points)

        EH_host <-
          EH_buffers %>%
          filter({{ host_ID }} == .y)

        suppressWarnings(
          st_intersection(geom, EH_host) %>%
          pull(.data$EH_property_ID))
      }))
  }


  ### TIDY TABLE CREATION ######################################################

  # Create tidy version of ghost_points

  if (multi_date) {

    if (!quiet) message("Producing final output table. (",
                        substr(Sys.time(), 12, 19), ")")

    points <-
      points[c("ghost_ID", "start", "end")] %>%
      st_drop_geometry() %>%
      mutate(date = map2(.data$start, .data$end, ~{
        seq(unique(.x), unique(.y), 1)
      })) %>%
      tidyr::unnest_legacy(.data$date) %>%
      select(-.data$start, -.data$end) %>%
      filter(.data$date >= start_date, .data$date <= end_date) %>%
      left_join(points, by = "ghost_ID") %>%
      select(-.data$start, -.data$end) %>%
      st_as_sf()

    # Remove rows from ghost_tidy which are in ghost_subset overlaps
    points <-
      points %>%
      group_by(.data$date) %>%
      filter(!.data$ghost_ID %in% unlist(.data$subsets)) %>%
      select(-.data$subsets) %>%
      ungroup()

  }

  total_time <- Sys.time() - time_1

  if (!quiet) {message("Analysis complete. (",
                       substr(Sys.time(), 12, 19), ")")}

  if (!quiet) {message("Total time: ",
                       substr(total_time, 1, 5), " ",
                       attr(total_time, "units"), ".")}

  return(points)
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
#' @return The output will be the `points`` object, rearranged with one row per
#'   cluster and with a new `predicates` field.
#' @importFrom dplyr %>% filter mutate
#' @importFrom purrr map map2
#' @importFrom rlang .data
#' @importFrom sf st_buffer st_intersects

ghost_cluster <- function(points, distance, min_listings) {

  # Create intersect predicate lists
  points <-
    points %>%
    mutate(
      predicates = map(.data$data, ~st_intersects(st_buffer(.x, distance))) %>%
        map(function(pred) {
               map(seq_along(pred), ~{
                 reduce(pred, function(x, y){
                   if (any(y %in% x)) unique(c(x, y)) else x
                 }, # Merge lists with common elements
                 .init = pred[[.]]) # Compile lists starting at each position
               }) %>%
                 map(sort) %>%
                 unique() # Remove duplicate lists
             }))

  # Remove lists < min_listings
  points <-
    points %>%
    mutate(predicates = map(.data$predicates, ~{.[lengths(.) >= min_listings]}))

  # Use predicates to split points into clusters with length >= min_listings
  points <-
    points %>%
    filter(map(.data$predicates, length) > 0) %>%
    mutate(data = map2(.data$data, .data$predicates, function(x, y) {
      map(y, ~{x[.,]})
      })) %>%
    tidyr::unnest_legacy(.data$data)

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
#' @param predicates The points$data field generated from st_intersects.
#' @param n A numeric scalar. The number of points to attempt to find a set of
#'   combinations for
#' @return The output will be a matrix of possible intersection combinations.
#' @importFrom dplyr %>%
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
  })) > 1e7) {

    # Establish collective centroid
    if (length(invalid_pr) > 0) {
      valid_bf <- buffers[-invalid_pr,]
    } else {valid_bf <- buffers}

    centroid <-
      valid_bf %>%
      st_centroid() %>%
      st_union() %>%
      st_centroid()

    # Remove furthest point from predicate list
    invalid_pr <- c(invalid_pr,
                    which.max(st_distance(st_centroid(valid_bf), centroid)))

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
#' @importFrom dplyr %>% as_tibble distinct mutate
#' @importFrom purrr map reduce
#' @importFrom rlang .data
#' @importFrom sf st_agr<- st_as_sf st_intersects st_is

ghost_stepwise_intersect <- function(buffers, min_listings) {

  # Deal with 0-length input
  if (nrow(buffers) == 0) return(buffers)

  # Build predicates and h_score
  predicates <- st_intersects(buffers)
  matrix <- matrix(
    c(sort(lengths(predicates), decreasing = TRUE), seq_along(predicates)),
    nrow=length(predicates)
  )
  h_score <- max(matrix[matrix[,2] >= matrix[,1],1])

  # Setup variables
  st_agr(buffers) = "constant"
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
      buffers[0,] %>%
        mutate(n.overlaps = as.integer(NA), origin = list(c(0)))
    )
  }

  # Master while-loop
  while (n >= min_listings) {

    # Try all combinations for a given n
    intersect_output <-
      map(combinations, function(x, n) {
        intersect <- suppressWarnings(
          split(buffers[x,], seq_len(nrow(buffers[x,]))) %>%
            reduce(ghost_intersect_with_done))
        intersect <- intersect[,1:cols]
        mutate(intersect, n.overlaps = n, origins = list(x))
      }, n = n)

    # Discard null results and rbind to single sf tibble
    intersect_output <-
      intersect_output[map(intersect_output, nrow) > 0] %>%
      do.call(rbind, .) %>%
      as_tibble()

    # Conditional to decide if the while-loop should continue
    if (nrow(intersect_output) == 0) {
      n <- n-1
      combinations <- ghost_combine(buffers, predicates, n)
    } else {
      intersect_output <-
        intersect_output %>%
        st_as_sf() %>%
        distinct(.data$geometry, .keep_all = TRUE)

      if (any(st_is(intersect_output, "POLYGON") == FALSE)) {
        stop("Invalid geometry produced")
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
#' @param property_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR listings.
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

ghost_intersect <- function(
  points, property_ID, distance, min_listings) {

  # Prepare buffers
  points <-
    points %>%
    mutate(buffers = map(.data$data, st_buffer, dist = distance))

  # Create intersects using ghost_stepwise_intersect
  points <-
    points %>%
    mutate(
      intersects = map(.data$buffers, ghost_stepwise_intersect, min_listings))

  # Remove empty clusters
  points <-
    points %>%
    filter(map(.data$intersects, nrow) > 0)

  # Choose intersect with max area
  points_to_add <-
    points %>%
    filter(map(.data$intersects, nrow) > 1) %>%
    mutate(intersects = map(.data$intersects, ~.x[which.max(st_area(.x)),]))

  # Consolidate list of clusters
  points <-
    points %>%
    filter(map(.data$intersects, nrow) == 1) %>%
    rbind(points_to_add)

  # Add $property_IDs field
  data_PIDs <- map(points$data, `$`, {{ property_ID }})
  int_origs <- map(points$intersects, `$`, "origins")
  points <-
    points %>%
    mutate(property_IDs = map2(int_origs, data_PIDs, ~{.y[.x[[1]]]}))

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
#' @param property_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR listings.
#' @param host_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR hosts.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be a set of new candidate `points`.
#' @importFrom dplyr %>% filter mutate select
#' @importFrom purrr map_int map2
#' @importFrom rlang .data

ghost_identify_leftovers <- function(points, property_ID, host_ID,
                                     min_listings) {
  points %>%
    filter(map_int(.data$data, nrow) - lengths(.data$property_IDs) >=
             min_listings) %>%
    mutate(data = map2(.data$data, .data$property_IDs, ~{
      filter(.x, !({{ property_ID }} %in% .y))
    })) %>%
    select({{ host_ID }}, .data$data)
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
#' @param property_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR listings.
#' @param host_ID The name of a character or numeric variable in the points
#'   object which uniquely identifies STR hosts.
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

ghost_intersect_leftovers <- function(points, property_ID, host_ID, distance,
                                      min_listings) {

  property_ID <- enquo(property_ID)
  host_ID <- enquo(host_ID)

  if (nrow(points) == 0) return(points)

  # Subset leftover candidates
  leftovers <- ghost_identify_leftovers(points, {{ property_ID }},
                                        {{ host_ID }}, min_listings)

  # Condition to run ghost_intersect on leftovers
  while (nrow(leftovers) > 0) {

    # Remove leftovers from points$data
    points <-
      rbind(
        points %>%
          filter(map_int(.data$data, nrow) - lengths(.data$property_IDs) <
                   min_listings),
        points %>%
          filter(map_int(.data$data, nrow) - lengths(.data$property_IDs) >=
                   min_listings) %>%
          mutate(data = map2(.data$data, .data$property_IDs, ~{
            filter(.x, {{ property_ID }} %in% .y)
          }))
      )

    # Apply ghost_intersect to leftovers
    leftover_outcome <- ghost_intersect(leftovers, {{ property_ID }}, distance,
                                        min_listings)

    # Add leftover_outcome to points and remove duplicate Property_IDs
    points <- rbind(points, leftover_outcome)

    # Subset leftover candidates again
    leftovers <- ghost_identify_leftovers(points, {{ property_ID }},
                                          {{ host_ID }}, min_listings)
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
#' @param points An sf data frame of STR listings nested by cluster.
#' @param crs_points The CRS of the points table.
#' @importFrom dplyr %>% everything mutate select
#' @importFrom sf st_as_sf st_set_crs
#' @importFrom rlang .data

ghost_empty <- function(points, crs_points) {
  points %>%
    mutate(ghost_ID = integer(0),
           date = as.Date(x = integer(0), origin = "1970-01-01")) %>%
    select(.data$ghost_ID, .data$date, everything()) %>%
    mutate(list_count = integer(0),
           housing_units = integer(0),
           property_IDs = list()) %>%
    select(-.data$data, .data$data) %>%
    mutate(geometry = st_sfc()) %>%
    st_as_sf() %>%
    st_set_crs(crs_points)
}
