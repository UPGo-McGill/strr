#' Function to identify STR ghost hotels
#'
#' \code{str_ghost} takes reported STR listing locations and identifies
#' possible "ghost hotels"--clusters of private-room STR listings operating in a
#' single building.
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
#' @param start_date A character string of format YYYY-MM-DD indicating the
#'   first date for which to run the analysis.
#' @param end_date A character string of format YYYY-MM-DD indicating the last
#'   date for which to run the analysis.
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#'   buffer which will be drawn around points to determine possible ghost hotel
#'   locations.
#' @param min_listings A numeric scalar. The minimum number of listings to
#'   be considered a ghost hotel.
#' @param cores A positive integer scalar. How many processing cores should be
#'   used to perform the computationally intensive numeric integration step?
#' @return The output will be a tidy ad dataframe of identified ghost hotels,
#'   organized with the following fields: `Ghost_ID`: an identifier for each
#'   unique ghost hotel cluster. `Date`: the date on which the ghost hotel was
#'   detected. `Host_ID` (or whatever name was passed to the Host_ID argument):
#'   The ID number of the host operating the ghost hotel. `Listing count`: how
#'   many separate listings comprised the ghost hotel. `Housing units`: an
#'   estimate of how many housing units the ghost hotel occupies, calculated as
#'   `ceiling(Listing_count / 4)`. `Property_IDs` (or whatever name was passed
#'   to the Property_ID argument): A list of the Property_ID values from the
#'   listings comprising the ghost hotel. `data`: a nested tibble of additional
#'   variables present in the points object. `geometry`: the polygons
#'   representing the possible locations of each ghost hotel.
#' @importFrom dplyr %>% filter
#' @importFrom methods is
#' @importFrom sf st_as_sf st_crs st_transform
#' @export

str_ghost <- function(points, start_date, end_date, distance = 200,
                      min_listings = 3, cores = 1) {

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

  # Convert points from sp
  if (is(points, "Spatial")) {
    points <- st_as_sf(points)
  }

  # Check that points is sf
  if (is(points, "sf") == FALSE) {
    stop("The object `points` must be of class sf or sp.")
  }

  # Convert points to tibble
  points <- as_tibble(points) %>% st_as_sf()

  # Convert start_date and end_date to date class
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)

  # Multi-threaded version
  if (cores >= 2) {

    ghost_points <- ghost_setup(points, start_date, end_date, min_listings)

    clusters <- pbapply::splitpb(nrow(ghost_points), cores, nout = 100)
    ghost_list <- lapply(clusters, function(x) ghost_points[x,])
    cl <- parallel::makeForkCluster(cores)

    ghost_points <-
      ghost_list %>%
      pbapply::pblapply(function(x) {
        x %>%
          ghost_cluster(distance, min_listings) %>%
          ghost_intersect(distance, min_listings) %>%
          ghost_intersect_leftovers(min_listings)
        }, cl = cl) %>%
      do.call(rbind, .)

    ghost_points <- ghost_make_table(ghost_points, start_date, end_date)
    ghost_points <- ghost_make_tidy(ghost_points, start_date, end_date)

    # Single-threaded version
    } else {
      ghost_points <- ghost_setup(points, start_date, end_date, min_listings)
      ghost_points <- ghost_cluster(ghost_points, distance, min_listings)
      ghost_points <- ghost_intersect(ghost_points, distance, min_listings)
      ghost_points <- ghost_intersect_leftovers(ghost_points, min_listings)
      ghost_points <- ghost_make_table(ghost_points, start_date, end_date)
      ghost_points <- ghost_make_tidy(ghost_points, start_date, end_date)
    }

  ghost_points
}
