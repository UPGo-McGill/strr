#' Raffle to assign STR listings to administrative units for spatial analysis
#'
#' A function for probablistically assigning STR listings to administrative
#' geographies (e.g. census tracts) based on reported latitude/longitude.
#' The function works by combining a known probability density function (e.g.
#' Airbnb's spatial obfuscation of listing locations) with an additional source
#' of information about possible listing locations--either population or housing
#' densities.


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

