#' Helper function to reduce predicate lists to minimal overlapping sublists
#'
#' @param x A predicate list, or other list of numeric vectors.
#' @return A list whose elements are the minimal overlapping sublists of 
#' elements of x.

reduce <- function(x) {
  
  out <- Reduce(function(a, n) {
    merge_index <- lapply(a, intersect, x[[n]])
    
    if (sum(lengths(merge_index)) > 0) {
      merge_index <- which(lengths(merge_index) > 0)
      merged <- a[merge_index]
      merged <- unlist(merged)
      merged <- union(merged, x[[n]])
      merged <- list(sort(merged))
      not_merged <- a[-merge_index]
      out <- c(merged, not_merged)
    } else out <- c(a, list(x[[n]]))
  }, seq_along(x), init = list())
  
  out
}


# Function to identify tables per date range
ghost_date_range <- function(data, dates, min_listings) {
  
  date_table <- data.table::data.table(
    start = dates[-length(dates)], end = dates[-1] - 1)
  
  date_table[, PID := mapply(\(start, end) sort(data[
    created <= start & scraped >= end]$property_ID), start, end)]
  
  date_table <- date_table[lengths(PID) >= min_listings]
  
  # Leave early if there aren't any valid rows
  if (nrow(date_table) == 0) return(NULL)
  
  date_table[, rle := data.table::rleid(sapply(PID, paste, collapse = " "))]
  
  date_table <- date_table[
    , .(start = min(start), end = max(end), 
        PID = list(sort(unique(unlist(PID))))), by = rle]
  
  date_table[, rle := NULL]
  
  data_update <- lapply(date_table$PID, \(x) data[property_ID %in% x])
  list(data = data_update, start = date_table$start, end = date_table$end)
  
}


#' Helper function to create potential ghost hostel clusters
#'
#' @param property A data frame of STR listings with sf or sp point geometries in
#'   a projected coordinate system.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible ghost hostel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The `property` table, rearranged with one row per cluster.

ghost_cluster <- function(property, distance, min_listings) {
  
  predicates <- data <- NULL
  
  # Create intersect predicate lists
  property[, predicates := lapply(data, function(x) {
    sf::st_intersects(sf::st_buffer(sf::st_as_sf(x), distance))
  })]
  
  # Extra list around lapply is needed to force embedded list where nrow == 1
  property[, predicates := list(lapply(predicates, reduce))]
  
  # Remove lists < min_listings
  property[, predicates := list(lapply(
    predicates, function(x) x[lengths(x) >= min_listings]))]
  
  # Use predicates to split property into clusters with length >= min_listings
  property <- property[lapply(predicates, length) > 0]
  
  pred_fun <- function(x, y) lapply(y, function(z) x[z,])
  
  property[, data := list(mapply(pred_fun, data, predicates, SIMPLIFY = FALSE))]
  
  # Exit function early if property table is empty
  if (nrow(property) == 0) {
    return(data.table::data.table(host_ID = character(), data = list()))
  }
  
  # Unnest data
  by_names <- if ("start" %in% names(property)) {
    c("host_ID", "start", "end") 
    } else "host_ID"
  
  property <- property[, .(data = unlist(data, recursive = FALSE)), 
                       by = by_names]
  
  # Remove duplicate clusters
  property <- property[!duplicated(property$data),]
  
  property
}


ghost_split_clusters <- function(property, distance, min_listings, 
                                 threshold = min_listings * 2) {
  
  # Find how many splitting candidates there are
  which_split <- sapply(property$data, nrow) >= threshold
  
  # Exit early if there are no candidates
  if (sum(which_split) == 0) return(property)
  
  property$data <- lapply(property$data, \(dat) {
    
    if (nrow(dat) < threshold) return(list(dat))
    
    min_length <- 0
    dist <- 0
    
    # Get minimum buffer distance that generates valid clusters for all points
    while (min_length < min_listings) {
      
      dist <- dist + 4
      pred <- reduce(sf::st_intersects(sf::st_buffer(dat$geometry, dist)))
      min_length <- min(lengths(pred))
      
    }
    dat <- lapply(pred, \(y) dat[y,])
    dat
    
  })
  
  by_names <- if ("start" %in% names(property)) {
    c("host_ID", "start", "end") 
  } else "host_ID"
  
  property[, .(data = unlist(data, recursive = FALSE)), by = by_names]
  
}

#' Helper function to create potential intersection combinations
#'
#' @param buffers A data frame of buffers from property$data.
#' @param predicates The property$data field generated from \code{st_intersects}.
#' @param n A numeric scalar. The number of points to attempt to find a set of
#'   combinations for
#' @return A matrix of possible intersection combinations.

ghost_combine <- function(buffers, predicates, n) {
  
  # Get valid buffers and predicates for combinations
  valid_pr <- predicates
  valid_pr2 <- valid_pr
  invalid_pr <- which(tabulate(unlist(valid_pr)) < n)
  if (length(invalid_pr) > 0) {
    valid_pr <- purrr::map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
  }
  
  while (!identical(valid_pr, valid_pr2)) {
    valid_pr2 <- valid_pr
    invalid_pr <- unique(c(invalid_pr, which(tabulate(unlist(valid_pr)) < n)))
    valid_pr <- purrr::map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
    if (length(valid_pr) < n) break
  }
  
  # Test if any valid combinations could exist, and exit if not
  if (length(valid_pr) < n) return(matrix(nrow = 0, ncol = 0))
  
  # Identify sets to generate combinations from
  combinations <- purrr::map(valid_pr, function(x){
    x[purrr::map(valid_pr, ~which(x %in% .)) %>%
        unlist() %>%
        tabulate() >= n]
  }) %>%
    unique()
  
  # Test if only a single valid combination could exist, and exit if so
  if (length(combinations) == 1) return(
    dplyr::tibble(value = combinations[[1]]))
  
  # Test if reducer will be necessary to avoid too many combinations
  while (
    is.nan(sum(purrr::map_dbl(combinations, ~{
      factorial(length(.)) / {factorial(n) * factorial(length(.) - n)}
    }))) || sum(purrr::map_dbl(combinations, ~{
      factorial(length(.)) / {factorial(n) * factorial(length(.) - n)}
    })) > 100000) {
    
    # Establish collective centroid
    if (length(invalid_pr) > 0) {
      valid_bf <- buffers[-invalid_pr,]
    } else {valid_bf <- buffers}
    
    centroid <-
      valid_bf %>%
      sf::st_centroid() %>%
      sf::st_union() %>%
      sf::st_centroid()
    
    # Identify furthest point
    to_remove <-
      which(buffers$property_ID == valid_bf[which.max(sf::st_distance(
        sf::st_centroid(valid_bf), centroid)),]$property_ID)
    
    # Remove furthest point from predicate list
    invalid_pr <- c(invalid_pr, to_remove)
    
    # Repeat previous steps to generate new combinations
    valid_pr <- purrr::map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
    while (!identical(valid_pr, valid_pr2)) {
      valid_pr2 <- valid_pr
      invalid_pr <- unique(c(invalid_pr, which(tabulate(unlist(valid_pr)) < n)))
      valid_pr <- purrr::map(predicates[-invalid_pr], ~{.[!(. %in% invalid_pr)]})
      if (length(valid_pr) < n) break
    }
    if (length(valid_pr) < n) return(matrix(nrow = 0, ncol = 0))
    combinations <- purrr::map(valid_pr, function(x){
      x[purrr::map(valid_pr, ~which(x %in% .)) %>%
          unlist() %>%
          tabulate() >= n]
    }) %>%
      unique()
  }
  
  # Generate combinations from valid buffers and discard duplicates
  combinations <-
    purrr::map(combinations, utils::combn, n) %>%
    do.call(cbind, .) %>%
    unique(MARGIN = 2)
  
  # Reduce combinations to valid options
  combinations <- combinations[, apply(combinations, 2, function(x) {
    sapply(valid_pr, function(y) all(x %in% y))
  }) %>%
    colSums() >= n]
  
  # Convert combinations matrix to tibble for future steps
  combinations <- suppressWarnings(dplyr::as_tibble(combinations))
  
  combinations
}


#' Helper function to intersect two buffers
#'
#' @param x A buffer.
#' @param y A buffer.
#' @return An intersect polygon.

ghost_intersect_with_done <- function(x, y) {
  result <- sf::st_intersection(x, y)
  if (nrow(result) == 0) rlang::done(result) else result
}


#' Helper function to intersect a data frame of buffers
#'
#' @param buffers A data frame of buffers from property$data.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return An intersect polygon.

ghost_stepwise_intersect <- function(buffers, min_listings) {
  
  # Deal with 0-length input
  if (nrow(buffers) == 0) return(buffers)
  
  # Build predicates and h_score
  predicates <- sf::st_intersects(buffers)
  matrix <-
    matrix(c(sort(lengths(predicates), decreasing = TRUE),
             seq_along(predicates)),
           nrow = length(predicates))
  h_score <- max(matrix[matrix[,2] >= matrix[,1],1])
  
  # Set up variables
  sf::st_agr(buffers) <- "constant"
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
    return(dplyr::mutate(buffers[0,], n.overlaps = as.integer(NA),
                         origin = list(c(0))))
  }
  
  # Master while-loop
  while (n >= min_listings) {
    
    # Try all combinations for a given n
    intersect_output <-
      purrr::map(combinations, function(x, n) {
        intersect <- suppressWarnings(
          split(sf::st_as_sf(data.table::setDF(buffers[x,])), 
                seq_len(nrow(buffers[x,]))) %>%
            purrr::reduce(ghost_intersect_with_done))
        intersect <- intersect[, 1:cols]
        dplyr::mutate(intersect, n.overlaps = n, origins = list(x))
      }, n = n)
    
    # Discard null results and rbind to single sf tibble
    intersect_output <-
      intersect_output[purrr::map(intersect_output, nrow) > 0] %>%
      do.call(rbind, .) %>%
      dplyr::as_tibble()
    
    # Conditional to decide if the while-loop should continue
    if (nrow(intersect_output) == 0) {
      n <- n - 1
      combinations <- ghost_combine(buffers, predicates, n)
    } else {
      intersect_output <-
        intersect_output %>%
        sf::st_as_sf() %>%
        dplyr::distinct(.data$geometry, .keep_all = TRUE)
      
      if (any(sf::st_is(intersect_output, "POLYGON") == FALSE)) {
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
#' @param property An sf data frame of STR listings nested by cluster.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible ghost hostel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be `property`, trimmed to valid ghost hostel locations
#'   and with additional fields added.

ghost_intersect <- function(property, distance, min_listings) {
  
  buffers <- data <- intersects <- property_IDs <- NULL
  
  # Prepare buffers
  property[, buffers := lapply(data, function(x) 
    sf::st_buffer(sf::st_as_sf(x), dist = distance))]
  
  # Otherwise create intersects using ghost_stepwise_intersect
  property[, intersects := purrr::map(buffers, ghost_stepwise_intersect, 
                                      min_listings)]
    
  # Remove empty clusters
  property <- property[lapply(intersects, nrow) > 0]
  
  # Choose intersect with max area
  property[lapply(intersects, nrow) > 1,
           intersects := lapply(intersects,
                                function(x) x[which.max(sf::st_area(x)),])]
  
  # Add $property_IDs field
  data_PIDs <- purrr::map(property$data, `$`, "property_ID")
  int_origs <- purrr::map(property$intersects, `$`, "origins")
  
  property[, property_IDs := mapply(function(x, y) y[x[[1]]], int_origs,
                                    data_PIDs, SIMPLIFY = FALSE)]
  
  property
}


#' Helper function to identify leftover ghost hostel candidates
#'
#' @param property An sf data frame of STR listings nested by cluster.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The output will be a set of new candidate points.

ghost_identify_leftovers <- function(property, min_listings) {
  
  data <- property_ID <- property_IDs <- host_ID <- NULL
  
  leftovers <-
    property[sapply(data, nrow) - lengths(property_IDs) >= min_listings]
  
  leftovers[, data := mapply(function(x, y) x[!x$property_ID %in% y,], data,
                             property_IDs, SIMPLIFY = FALSE)]
  
  if (nrow(leftovers) == 0) return(leftovers)
  
  # Multi_date
  if ("start" %in% names(leftovers)) {
    leftovers <- leftovers[, .(host_ID, start, end, data)]
    leftovers[, PID := sapply(data, \(x) {
      paste(x$property_ID, collapse = " ")
    })]
    leftovers[, rle := data.table::rleid(PID)]
    
    leftovers <- leftovers[, .(host_ID = host_ID[1], start = min(start), 
                               end = max(end), data = data[1]), by = rle]
    leftovers[, rle := NULL]
    
  } else {
    leftovers <- leftovers[, .(host_ID, data)]
    leftovers <- leftovers[!duplicated(leftovers$data)]
  }

  leftovers
}


#' Helper function to rerun analysis on leftover ghost hostel candidates
#'
#' @param property An sf data frame of STR listings nested by cluster.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around property to determine possible ghost hostel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hostel.
#' @return The `property` file with additional ghost hostels added.

ghost_intersect_leftovers <- function(property, distance, min_listings) {
  
  data <- property_IDs <- property_ID <- NULL
  
  if (nrow(property) == 0) return(property)
  
  # Subset leftover candidates
  leftovers <- ghost_identify_leftovers(property, min_listings)
  
  # Condition to run ghost_intersect on leftovers
  while (nrow(leftovers) > 0) {
    
    # Remove leftovers from property$data
    property <-
      rbind(
        property[sapply(data, nrow) - lengths(property_IDs) < min_listings],
        # Add leftovers in case there is only a partial match
        property[sapply(data, nrow) - lengths(property_IDs) >= min_listings
        ][, data := mapply(function(x, y) x[x$property_ID %in% y,], data,
                           property_IDs, SIMPLIFY = FALSE)])
    
    # Apply ghost_intersect to leftovers
    leftover_outcome <- ghost_intersect(leftovers, distance, min_listings)
    
    # Add leftover_outcome to property
    property <- rbind(property, leftover_outcome)
    
    # Subset leftover candidates again
    leftovers <- ghost_identify_leftovers(property, min_listings)
  }
  
  property
}


#' Helper function to output an empty ghost hostel table
#'
#' @param crs_property The CRS of the property table.

ghost_empty <- function(crs_property) {
  dplyr::tibble(ghost_ID = integer(),
                date = as.Date(x = integer(), origin = "1970-01-01"),
                host_ID = character(),
                listing_count = integer(),
                housing_units = integer(),
                property_IDs = list(),
                data = list(),
                geometry = sf::st_sfc()) %>%
    sf::st_as_sf(crs = crs_property)
}
