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
  property <- property[lengths(predicates) > 0]
  
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
  
  # Exit early if output is empty
  if (nrow(property) == 0) return(property)
  
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
  
  if (
    length(lengths(property$data)) > 1 ||
    (length(lengths(property$data)) == 1 && 
     !data.table::is.data.table(property$data[[1]]))
    ) {
    property <- 
      property[, .(data = unlist(data, recursive = FALSE)), by = by_names]
  }
    
  
  return(property)
  
}


ghost_dist_matrix <- function(buffers) {
  out <- suppressWarnings(sf::st_centroid(sf::st_as_sf(buffers)))
  coords <- sf::st_coordinates(out)
  out$x <- coords[,1]
  out$y <- coords[,2]
  out <- sf::st_drop_geometry(out)
  out <- out[, c("property_ID", "x", "y")]
  out <- as.data.frame(out)
  mat <- distances::distances(out, id_variable = "property_ID",
                              dist_variables = c("x", "y"))
  as.matrix(mat)
}


ghost_intersects <- function(buf, min_listings) {
  
  dist_mat <- ghost_dist_matrix(buf)
  predicates <- sf::st_intersects(buf)
  pred_square <- length(unlist(predicates)) / length(predicates) ^ 2
  
  while (pred_square < 1 & nrow(buf) >= min_listings) {
    
    # Remove point with highest mean inter-point distance
    to_remove <- which.max(colMeans(dist_mat))
    buf <- buf[-to_remove,]
    dist_mat <- dist_mat[-to_remove, -to_remove]
    predicates <- sf::st_intersects(buf)
    pred_square <- length(unlist(predicates)) / length(predicates) ^ 2
    
  }
  
  if (nrow(buf) < min_listings) return(character()) else return(buf$property_ID)
  
}


ghost_find_intersect <- function(property, distance, min_listings, geom_type) {
  
  # Return empty output if input is empty
  if (nrow(property) == 0) return(property)
  
  # Prepare buffers
  property[, buffers := lapply(data, function(x) 
    sf::st_buffer(sf::st_as_sf(x), dist = distance))]
  
  # Decide on geometry type
  if (geom_type == "point") geom_fun <- function(data, property_IDs, distance) {
    sf::st_centroid(sf::st_union(data[property_ID %in% property_IDs]$geometry))
  } else geom_fun <- function(data, property_IDs, distance) {
    buf <- sf::st_buffer(sf::st_as_sf(data[property_ID %in% property_IDs]), 
                         dist = distance)
    sf::st_sfc(Reduce(sf::st_intersection, buf$geometry))
  }
  
  # Save crs
  crs <- sf::st_crs(property$data[[1]])
  
  # Find intersects
  property[, property_IDs := lapply(buffers, ghost_intersects, min_listings)]
  
  # Remove clusters with too few intersections
  property <- property[lengths(property_IDs) >= min_listings]
  
  # Apply geometries
  property[, geometry := sf::st_sfc(
    mapply(geom_fun, data, property_IDs, MoreArgs = list(distance = distance)),
    crs = crs)]
  
  # Return output
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

ghost_find_leftovers <- function(property, distance, min_listings, geom_type) {
  
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
    leftover_outcome <- ghost_find_intersect(leftovers, distance, min_listings, 
                                             geom_type)
    
    # Add leftover_outcome to property
    if (nrow(leftover_outcome) > 0) property <- rbind(property, leftover_outcome)
    
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
