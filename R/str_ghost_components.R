

## 1.2. Create possible cluster start and end dates ----------------------------

ghost_make_starts <- function(x) {
  x %>%
    filter(Created > start) %>%
    `$`("Created") %>%
    unique() %>%
    c(start)
}

ghost_make_ends <- function(x) {
  x %>%
    filter(Scraped < end) %>%
    `$`("Scraped") %>%
    unique() %>%
    c(end)
}

## 1.3. Create tibbles for each unique cluster ---------------------------------

ghost_nest <- function(x, y) {
  apply(x, 1, function(z) filter(y, Created <= z[1], Scraped >= z[2])) %>%
    unique() %>%
    `[`(lapply(., nrow) >= min_list)
}

## 1.4. Set up points, returns `points` ----------------------------------------

ghost_setup <- function(
  points, start = start_date, end = end_date, min_list = min_listings) {

  # Remove invalid listings
  points <-
    points %>%
    filter(Host_ID != 0,
           is.na(Host_ID) == FALSE,
           Listing_Type == "Private room")

  # Filter points to PR points >= min_listings, and nest by Host_ID
  points <-
    points %>%
    arrange(Host_ID, Property_ID) %>%
    group_by(Host_ID) %>%
    filter(n() >= min_list) %>%
    nest()

  # Identify possible clusters by date
  points <-
    points %>%
    mutate(
      starts = map(data, ghost_make_starts),
      ends = map(data, ghost_make_ends),
      date_grid = map2(starts, ends, expand.grid),
      date_grid = map(date_grid, filter, Var1 <= Var2)
    )

  # Create a nested tibble for each possible cluster
  points <-
    points %>%
    mutate(data = map2(date_grid, data, ghost_nest)) %>%
    unnest(data)

  points
}

### 2. GHOST HOTEL COMPONENT FUNCTIONS -----------------------------------------

## 2.1 Create valid clusters, returns `points` ---------------------------------

ghost_cluster <- function(points, dist = distance, min_list = min_listings) {

  # Create intersect predicate lists
  points <-
    points %>%
    mutate(predicates = map(data, ~st_intersects(st_buffer(., dist))) %>%
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
    mutate(predicates = map(predicates, ~{.[lengths(.) >= min_list]}))

  # Use predicates to split points into clusters with length >= min_listings
  points <- points %>%
    filter(map(predicates,length)>0) %>%
    mutate(data = map2(data, predicates, function(x, y) map(y, ~{x[.,]}))) %>%
    unnest(data)

  # Remove duplicate clusters
  points <- points[!duplicated(points$data),]

  points
}

## 2.2 Generate combinations, returns `combinations` ---------------------------

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
    invalid_pr <- c(invalid_pr, which(
      buffers$Property_ID == valid_bf[which.max(
        st_distance(st_centroid(valid_bf),centroid)),]$Property_ID
    ))

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
    combinations %>%
    map(combn,n) %>%
    do.call(cbind,.) %>%
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

## 2.3 Intersect two buffers with done(), to use in reduce() -------------------

ghost_intersect_with_done <- function(x, y) {
  result <- st_intersection(x, y)
  if (nrow(result) == 0) done(result) else result
}

## 2.4 Stepwise st_intersection implementation, returns `intersect_output` -----

ghost_stepwise_intersect <- function(buffers, min_list = min_listings) {

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
  while (ncol(combinations) == 0 & n >= min_list) {
    n <- n - 1
    combinations <- ghost_combine(buffers, predicates, n)
  }

  # Exit function if no valid combinations exist
  if (n < min_list) {
    return(
      buffers[0,] %>%
        mutate(n.overlaps = as.integer(NA), origin = list(c(0)))
    )
  }

  # Master while-loop
  while (n >= min_list) {

    # Try all combinations for a given n
    intersect_output <-
      map(combinations, function(x, n) {
        intersect <- suppressWarnings(
          split(buffers[x,],buffers[x,]$Property_ID) %>%
            reduce(ghost_intersect_with_done))
        intersect <- intersect[,1:cols]
        mutate(intersect, n.overlaps = n, origins = list(x))
      }, n = n)

    # Discard null results and rbind to single sf tibble
    intersect_output <-
      intersect_output[map(intersect_output, nrow) > 0] %>%
      rbindlist() %>%
      as_tibble()

    # Conditional to decide if the while-loop should continue
    if (nrow(intersect_output) == 0) {
      n <- n-1
      combinations <- ghost_combine(buffers, predicates, n)
    } else {
      intersect_output <-
        intersect_output %>%
        st_as_sf() %>%
        distinct(geometry, .keep_all = TRUE)

      if (any(st_is(intersect_output, "POLYGON") == FALSE)) stop()

      return(intersect_output)
    }
  }

  intersect_output
}


# 2.5 Find ghost hotel locations, returns `points` -----------------------------

ghost_intersect <- function(points, dist = distance, min_list = min_listings) {

  # Prepare buffers
  points <-
    points %>%
    mutate(buffers = map(data, st_buffer, dist = dist))

  # Create intersects using ghost_stepwise_intersect
  points <-
    points %>%
    mutate(intersects = map(buffers, ghost_stepwise_intersect_new, min_list))

  # Remove empty clusters
  points <-
    points %>%
    filter(map(intersects, nrow) > 0)

  # Choose intersect with max area
  points_to_add <-
    points %>%
    filter(map(intersects, nrow) > 1) %>%
    mutate(intersects = map(intersects, ~.x[which.max(st_area(.x)),]))

  # Consolidate list of clusters
  points <-
    points %>%
    filter(map(intersects, nrow) == 1) %>%
    rbind(points_to_add) %>%
    arrange(Host_ID)

  # Add $Property_IDs field
  data_PIDs <- map(points$data, `$`, "Property_ID")
  int_origs <- map(points$intersects, `$`, "origins")
  points <-
    points %>%
    mutate(Property_IDs = map2(int_origs, data_PIDs, ~{.y[.x[[1]]]}))

  points
}

# 2.6 Identify leftover candidates, returns `leftovers` ------------------------

ghost_identify_leftovers <- function(points, min_list) {
  points %>%
    filter(map_int(data, nrow) - lengths(Property_IDs) >= min_list) %>%
    mutate(data = map2(data, Property_IDs, ~{
      filter(.x, !(Property_ID %in% .y))
    })) %>%
    select(Host_ID, data)
}

# 2.7 Rerun analysis on leftover points, returns `points` ----------------------

ghost_intersect_leftovers <- function(points, min_list = min_listings) {

  # Subset leftover candidates
  leftovers <- ghost_identify_leftovers(points, min_list)

  # Condition to run ghost_intersect on leftovers
  while (nrow(leftovers) > 0) {

    # Remove leftovers from points$data
    points <-
      rbind(
        points %>%
          filter(map_int(data, nrow) - lengths(Property_IDs) < min_list),
        points %>%
          filter(map_int(data, nrow) - lengths(Property_IDs) >= min_list) %>%
          mutate(data = map2(data, Property_IDs, ~{
            filter(.x, Property_ID %in% .y)
          }))
      ) %>%
      arrange(Host_ID)

    # Apply ghost_intersect to leftovers
    leftover_outcome <- ghost_intersect(leftovers)

    # Add leftover_outcome to points and remove duplicate Property_IDs
    points <- rbind(points, leftover_outcome) %>% arrange(Host_ID)

    # Subset leftover candidates again
    leftovers <- ghost_identify_leftovers(points, min_list)
  }

  points
}

# 2.8 Assemble ghost hotels, returns `ghost_points` ----------------------------

ghost_make_table <- function(points, start = start_date, end = end_date) {

  # Remove unused listings from $data
  ghost_points <-
    points %>%
    mutate(data = map2(data, Property_IDs, ~{filter(.x, Property_ID %in% .y)}))

  # Store CRS for later
  crs <- st_crs(ghost_points$intersects[[1]])

  # Generate compact table of ghost hotels, suppress SF geometry warnings
  ghost_points <- suppressWarnings(
    unnest(ghost_points, intersects, .preserve = data)
  )

  # Remove duplicates
  ghost_points <- ghost_points[!duplicated(ghost_points$Property_IDs),]

  # Create Ghost_ID
  ghost_points <- mutate(ghost_points, Ghost_ID = 1:n())

  # Extract geometry, coerce to sf, join back to ghost_points, clean up
  ghost_points <-
    ghost_points[c("Ghost_ID","geometry")] %>% st_as_sf() %>%
    left_join(
      ghost_points[, names(ghost_points) != "geometry"], ., by = "Ghost_ID") %>%
    st_as_sf() %>%
    rename(Listing_count = n.overlaps) %>%
    mutate(Housing_units = as.integer(ceiling(Listing_count / 4))) %>%
    select(Ghost_ID, Host_ID, Listing_count,
           Housing_units, Property_IDs, data, geometry)

  # Reattach CRS
  st_crs(ghost_points) <- crs

  # Calculate date ranges for ghost_points
  ghost_points <-
    ghost_points %>%
    mutate(
      Start = map_dbl(data, ~max(c(.x$Created), start)) %>%
        as.Date(origin = "1970-01-01"),
      End = map_dbl(data, ~min(c(.x$Scraped), end)) %>%
        as.Date(origin="1970-01-01"))

  # Identify subsets
  ghost_points <-
    ghost_points %>%
    mutate(
      Subsets = map(Property_IDs, function(y) {
        which(map_lgl(ghost_points$Property_IDs, ~all(.x %in% y)))
      }),
      Subsets = map2(Ghost_ID, Subsets, ~{.y[.y != .x]}))

  ghost_points
}

# 2.9 Produce tidy table of ghost hotels, returns `ghost_tidy` -----------------

ghost_make_tidy <- function(ghost_points, start = start_date, end = end_date){

  # Create tidy version of ghost_points
  ghost_tidy <-
    ghost_points[c("Ghost_ID","Start","End")] %>%
    st_drop_geometry() %>%
    mutate(Date = map2(Start, End, ~seq(unique(.x), unique(.y), 1))) %>%
    unnest(Date) %>%
    select(-Start, -End) %>%
    filter(Date >= start, Date <= end) %>%
    left_join(ghost_points, by = "Ghost_ID") %>%
    select(-Start, -End) %>%
    st_as_sf()

  # Remove rows from ghost_tidy which are in ghost_subset overlaps
  ghost_tidy <-
    ghost_tidy %>%
    group_by(Date) %>%
    filter(!Ghost_ID %in% unlist(Subsets)) %>%
    select(-Subsets) %>%
    ungroup()

  ghost_tidy
}
