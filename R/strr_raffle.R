#' Raffle to assign STR listings to administrative units for spatial analysis
#'
#' \code{strr_raffle} takes reported STR listing locations and assigns the
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
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be the input points object with a new `winner` field
#'   appended. The `winner` field specifies which polygon from the polys object
#'   was probabilistically assigned to the listing, using the field identified
#'   in the `poly_ID` argument. If diagnostic == TRUE, a `candidates` field will
#'   also be appended, which lists the possible polygons for each point, along
#'   with their probabilities.
#' @importFrom dplyr %>% as_tibble enquo filter group_by left_join mutate
#' @importFrom dplyr select summarize
#' @importFrom methods is
#' @importFrom rlang := .data
#' @importFrom sf st_area st_as_sf st_buffer st_coordinates st_crs
#' @importFrom sf st_drop_geometry st_intersection st_set_agr st_sfc
#' @importFrom sf st_transform
#' @importFrom stats dnorm
#' @export

strr_raffle <- function(
  points, polys, poly_ID, units, distance = 200, diagnostic = FALSE,
  cores = 1, quiet = FALSE) {

  time_1 <- Sys.time()

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

  # Initialize helper fields
  points <-
    points %>%
    mutate(.point_x = st_coordinates(.)[,1],
           .point_y = st_coordinates(.)[,2],
           .point_ID = seq_len(nrow(points)))

  # Clean up polys and initialize poly_area field
  polys <-
    polys %>%
    filter(!! units > 0) %>%
    st_set_agr("constant") %>% # Prevent warnings from the st operations
    mutate(
      {{ poly_ID }} := as.character({{ poly_ID }}), # Make sure poly_ID isn't factor; TKTK move to error checking
      poly_area = st_area(.) # Calculate polygon areas
    ) %>%
    st_set_agr("constant")

  # Generate buffers and intersect with polygons
  intersects <-
    points %>%
    st_buffer(dist = distance, nQuadSegs = 10) %>%
    st_set_agr("constant") %>%
    st_intersection(polys)

  # Estimate int_units
  intersects <-
    intersects %>%
    mutate(
      int_units = as.numeric({{ units }} * st_area(.) / .data$poly_area)) %>%
    st_set_agr("constant")

  # Transform intersects relative to point coordinates
  intersects <-
    intersects %>%
    mutate(geometry = purrr::pmap(
      list(.data$geometry, .data$.point_x, .data$.point_y), ~{
        ..1 - c(..2, ..3)
        }) %>%
        st_sfc())

  # Multi-threaded version of integration
  if (cores >= 2) {

    clusters <- pbapply::splitpb(nrow(intersects), cores, nout = 100)
    intersects_list <- lapply(clusters, function(x) intersects[x,])
    cl <- parallel::makeForkCluster(cores)

    intersects <-
      intersects_list %>%
      pbapply::pblapply(raffle_integrate, cl = cl) %>%
      do.call(rbind, .)

    # Single-threaded version of integration
    } else {
      intersects <- raffle_integrate(intersects)
    }

  # Initialize results object
  results <-
    intersects %>%
    st_drop_geometry() %>%
    group_by(.data$.point_ID)

  # Choose winners
  if (diagnostic == TRUE) {
    results <-
      results %>%
      summarize(
        winner = as.character(
          base::sample(!! poly_ID, 1, prob = .data$probability)),
        candidates = list(matrix(
          c(!! poly_ID, (.data$probability) / sum(.data$probability)),
          ncol = 2))
      )
  } else {
    results <-
      results %>%
      summarize(
        winner = as.character(
          base::sample({{ poly_ID }}, 1, prob = .data$probability))
      )
  }

  # Join winners to point file
  points <-
    left_join(points, results, by = ".point_ID") %>%
    select(-.data$.point_ID, -.data$.point_x, -.data$.point_y)

  helper_progress_message("Analysis complete.", .final = TRUE)
  points
}


#' Helper function specifying the probability density function
#'
#' \code{raffle_pdf} specifies the probability density function which Airbnb
#' uses to obfuscate listing locations.
#'
#' A function which specifies the probability density function which Airbnb
#' uses to obfuscate listing locations (a radially symmetric normal distribution
#' with mean 100 m and standard deviation 50 m, truncated at r = 0 and r = 200).
#' In the future, other PDFs may be added.
#'
#' @param x An st_coordinates object, converted to sp.
#' @return The output will be a numerical vector of probabilities.
#' @importFrom stats dnorm

raffle_pdf <- function(x) {
  dnorm(sqrt(x[,1]^2 + x[,2]^2), mean = 100, sd = 50, log = FALSE) *
    (1 / (2 * pi))
}


#' Helper function to integrate raffle_PDF over intersect polygons
#'
#' \code{raffle_integrate} takes raffle_PDF and integrates it over the intersect
#' polygons using polyCub.
#'
#' A function for integrating raffle_PDF (Airbnb's spatial obfuscation of
#' listing locations) over the intersect polygons using polyCub. In the future,
#' other PDFs may be added.
#'
#' @param intersects An sf data frame.
#' @return The output will be the input data frame with a `probability` field
#' added.
#' @importFrom dplyr %>% mutate
#' @importFrom rlang .data

raffle_integrate <- function(intersects) {
  intersects %>%
    mutate(probability = purrr::map2_dbl(.data$geometry, .data$int_units, ~{
      polyCub::polyCub.midpoint(as(.x, "Spatial"), raffle_pdf) * .y
    }))
}
