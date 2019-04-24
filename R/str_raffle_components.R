#' Raffle to assign STR listings to administrative units for spatial analysis
#' 
#' A function for probablistically assigning STR listings to administrative
#' geographies (e.g. census tracts) based on reported latitude/longitude.
#' The function works by combining a known probability density function (e.g.
#' Airbnb's spatial obfuscation of listing locations) with an additional source
#' of information about possible listing locations--either population or housing
#' densities.


## 1. Load libraries -----------------------------------------------------------

lapply(c("sf","dplyr","spatstat","polyCub", "purrr"),
       library, character.only = TRUE)


## 2. Point setup function, returns points -------------------------------------

raffle_setup_points <- function(points, point_ID) {
  
  points <- 
    points %>%
    filter(!! point_ID > 0) %>%
    arrange(!! point_ID)
  
  points
}


## 3. Polygon setup function, returns polys ------------------------------------

raffle_setup_polys <- function(polys, poly_ID, units){

  polys <- 
    polys %>%
    filter(!! units > 0) %>%
    st_set_agr("constant") %>% # Prevent warnings from the st operations
    mutate(
      !! poly_ID := as.character(!! poly_ID), # Make sure poly_ID is not factor
      poly_area = st_area(.) # Calculate polygon areas
    ) %>% 
    st_set_agr("constant")
  
  polys
}


## 4. Intersect point buffers with polygons, returns intersects ----------------

raffle_intersect <- function(points, polys, units, distance) {
  
  units <- enquo(units)
  
  # Get point coordinates for future use
  points <- 
    points %>%
    mutate(
      point_x = st_coordinates(.)[,1], 
      point_y = st_coordinates(.)[,2]
      )
  
  # Generate buffers and intersect with polygons
  intersects <-
    points %>%
    st_buffer(dist = distance, nQuadSegs = 10) %>% 
    st_set_agr("constant") %>%
    st_intersection(polys)
  
  # Estimate int_units
  intersects <-
    intersects %>%
    mutate(int_units = as.numeric(!! units * st_area(geometry) / poly_area)) %>% 
    st_set_agr("constant")
  
  # Transform intersects relative to point coordinates
  intersects <- 
    intersects %>% 
    mutate(geometry = pmap(list(geometry, point_x, point_y), ~{
      ..1 - c(..2, ..3)
      }) %>% 
        st_sfc())
  
  intersects
}


## 5. PDF helper function, returns vector of probabilities ---------------------

raffle_pdf <- function(x) {
  dnorm(sqrt(x[,1]^2 + x[,2]^2), mean = 100, sd = 50, log = FALSE) *
    (1 / (2 * pi))
}

## 6. Integrate the PDF over intersect polygons, returns intersects ------------

raffle_integrate <- function(intersects) {
  
  intersects <- 
    intersects %>% 
    mutate(probability = map2_dbl(geometry, int_units, ~{
      polyCub.midpoint(as(.x, "Spatial"), raffle_pdf) * .y
    })
    )
  
  intersects
}


## 7. Determine winners, returns points ----------------------------------------

raffle_choose_winner <- function(
  points, intersects, point_ID, poly_ID, diagnostic) {
  
  point_ID <- enquo(point_ID)
  poly_ID  <- enquo(poly_ID)
  
  results <-
    intersects %>%
    st_drop_geometry() %>%
    group_by(!! point_ID)
  
  if (diagnostic == TRUE) {
    results <-
      results %>%
      summarize(
        winner = as.character(base::sample(!! poly_ID, 1, prob = probability)),
        candidates = list(matrix(
          c(!! poly_ID, (probability) / sum(probability)), ncol = 2))
      )
  } else {
    results <-
      results %>%
      summarize(
        winner = as.character(base::sample(!! poly_ID, 1, prob = probability))
      )
  }
  
  points <- left_join(points, results, by = quo_name(point_ID))
  
  points
}


## 8. Main compiler to run the whole process -----------------------------------

str_raffle <- function(
  points, polys, point_ID, poly_ID, units,
  distance = 200, diagnostic = FALSE, cores = 1) {
  
  lapply(c("sf","dplyr","spatstat","polyCub", "purrr"), 
         library, character.only = TRUE)
  
  # Check that cores is an integer > 0
  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }
  
  # Check that distance > 0
  if (distance <= 0) {
    stop("The argument `distance` must be a positive number.")
  }
  
  # Convert points and polys from sp
  if (is(points, "Spatial")) {
    points <- st_as_sf(points)
  }
  if (is(polys, "Spatial")) {
    polys <- st_as_sf(polys)
  }
  
  # Check that points and polys are sf
  if (is(points, "sf") == FALSE) {
    stop("The object `points` must be of class sf or sp.")
  }
  if (is(polys, "sf") == FALSE) {
    stop("The object `polys` must be of class sf or sp.")
  }
  
  # Convert points and polys to tibble
  points <- as_tibble(points) %>% st_as_sf()
  polys  <- as_tibble(polys)  %>% st_as_sf()
  
  if (cores >= 2) library(parallel)
  if (cores >= 2) library(pbapply)
  
  point_ID <- enquo(point_ID)
  poly_ID  <- enquo(poly_ID)
  units    <- enquo(units)
  
  points <- raffle_setup_points(points, point_ID)
  polys <- raffle_setup_polys(polys, poly_ID, units)
  intersects <- raffle_intersect(points, polys, Housing, distance)

  # Multi-threaded version
  if (cores >= 2) {
    
    clusters <- splitpb(nrow(intersects), cores, nout = 100)
    intersects_list <- lapply(clusters, function(x) intersects[x,])
    cl <- makeForkCluster(cores)
    
    intersects <-
      intersects_list %>%
      pblapply(raffle_integrate, cl = cl) %>%
      do.call(rbind, .)
    
    # Single-threaded version 
    } else {
      intersects <- raffle_integrate(intersects)
      }
  
  points <-
    raffle_choose_winner(points, intersects, Property_ID, GEOID, diagnostic)
  
  points
}

