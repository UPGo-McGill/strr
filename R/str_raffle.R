#' Raffle to assign STR listings to administrative units for spatial analysis
#'
#' \code{str_raffle} takes reported STR listing locations and assigns the
#' listings to administrative units based on a probability density function and
#' other information about population or housing distribution.
#'
#' A function for probablistically assigning STR listings to administrative
#' geographies (e.g. census tracts) based on reported latitude/longitude.
#' The function works by combining a known probability density function (e.g.
#' Airbnb's spatial obfuscation of listing locations) with an additional source
#' of information about possible listing locations--either population or housing
#' densities.
#'
#' @param points An sf or sp point-geometry object, in a projected coordinate
#'   system.
#' @param polys An sf or sp polygon-geometry object with administrative
#'   geographies as polygons. It will be transformed into the CRS of `points`.
#' @param poly_ID The name of a character or numeric variable in the polys
#'   object to be used as an ID to identify the "winning" polygon assigned to
#'   each point in the output.
#' @param units The name of a numeric variable in the polys object which
#'   contains the weighting factor (number of people or housing units).
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible listing
#'   locations.
#' @param diagnostic A logical scalar. Should a list of polygon candidates and
#'   associated probabilities be appended to the function output?
#' @param cores A positive integer scalar. How many processing cores should be
#'   used to perform the computationally intensive numeric integration step?
#' @return The output will be the input points object with a new `winner` field
#'   appended. The `winner` field specifies which polygon from the polys object
#'   was probabilistically assigned to the listing, using the field identified
#'   in the `poly_ID` argument. If diagnostic == TRUE, a `candidates` field will
#'   also be appended, which lists the possible polygons for each point, along
#'   with their probabilities.

str_raffle <- function(
  points, polys, poly_ID, units, distance = 200, diagnostic = FALSE,
  cores = 1) {

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

  # Transform polys CRS to match points
  if (st_crs(points) != st_crs(polys)) {
    polys <- st_transform(polys, st_crs(points))
    }

  if (cores >= 2) library(parallel)
  if (cores >= 2) library(pbapply)

  poly_ID  <- enquo(poly_ID)
  units    <- enquo(units)

  points <- raffle_setup_points(points)
  polys <- raffle_setup_polys(polys, poly_ID, units)
  intersects <- raffle_intersect(points, polys, units, distance)

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
    raffle_choose_winner(points, intersects, poly_ID, diagnostic)

  points
}

