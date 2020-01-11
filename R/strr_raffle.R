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
#'   system. If the data frame does not have spatial attributes, an attempt will
#'   be made to convert it to sf using \code{\link{strr_as_sf}}. The result will
#'   be transformed into the Web Mercator projection (EPSG: 3857) for distance
#'   and area calculations. To use a projection more suitable to the data,
#'   supply an sf or sp object.
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
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be the input points object with a new `poly_ID` field
#'   appended. The `winner` field specifies which polygon from the polys object
#'   was probabilistically assigned to the listing, taking the name of the field
#'   identified in the `poly_ID` argument. If diagnostic == TRUE, a `candidates`
#'   field will also be appended, which lists the possible polygons for each
#'   point, along with their probabilities.
#' @importFrom dplyr %>% as_tibble enquo everything filter group_by left_join
#' @importFrom dplyr mutate select summarize
#' @importFrom rlang := .data
#' @importFrom sf st_area st_as_sf st_buffer st_coordinates st_crs
#' @importFrom sf st_drop_geometry st_intersection st_set_agr st_sfc
#' @importFrom sf st_transform
#' @importFrom stats dnorm
#' @export

strr_raffle <- function(
  points, polys, poly_ID, units, distance = 200, diagnostic = FALSE,
  quiet = FALSE) {

  time_1 <- Sys.time()


  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  ## Remove future global export limit

  options(future.globals.maxSize = +Inf)
  on.exit(.Options$future.globals.maxSize <- NULL)


  ## Check distance, diagnostic and quiet flags

  # Check that distance > 0
  if (distance <= 0) {
    stop("The argument `distance` must be a positive number.")
  }

  # Check that diagnostic is a logical
  if (!is.logical(diagnostic)) {
    stop("The argument `diagnostic` must be a logical value (TRUE or FALSE).")
  }

  # Check that quiet is a logical
  if (!is.logical(quiet)) {
    stop("The argument `quiet` must be a logical value (TRUE or FALSE).")
  }


  ## Handle spatial attributes

  # Convert points and polys from sp
  if (inherits(points, "Spatial")) {
    points <- st_as_sf(points)
  }
  if (inherits(polys, "Spatial")) {
    polys <- st_as_sf(polys)
  }

  # Check that points is sf, and convert to sf if possible
  if (!inherits(points, "sf")) {
    tryCatch({
      points <- strr_as_sf(points, 3857)
      helper_progress_message("Converting input table to sf.")
      },
    error = function(e) {
      stop(paste0("The object `points` must be of class sf or sp, ",
                  "or must be convertable to sf using strr_as_sf."))
    })
  }

  # Check that points and polys are sf
  if (inherits(points, "sf") == FALSE) {
    stop(paste0("The object `points` must be of class sf or sp, ",
                "or must be convertable to sf using strr_as_sf."))
  }
  if (inherits(polys, "sf") == FALSE) {
    stop("The object `polys` must be of class sf or sp.")
  }

  ## Check that polys fields exist

  tryCatch(
    pull(polys, {{ poly_ID }}),
    error = function(e) stop(
      "The value of `poly_ID` is not a valid field in the `polys` input table."
      ))

  tryCatch(
    pull(polys, {{ units }}),
    error = function(e) stop(
      "The value of `units` is not a valid field in the `polys` input table."
      ))


  ### PREPARE POINTS AND POLYS TABLES ##########################################

  helper_progress_message("Preparing tables for analysis.")

  # Transform polys CRS to match points
  polys <- st_transform(polys, st_crs(points))

  # Initialize helper fields
  points <-
    points %>%
    mutate(.point_x = st_coordinates(.)[,1],
           .point_y = st_coordinates(.)[,2],
           .point_ID = seq_len(nrow(points)))

  # Clean up polys and initialize poly_area field
  polys <-
    polys %>%
    filter({{ units }} > 0) %>%
    # Prevent warnings from the st operations
    st_set_agr("constant") %>%
    mutate(
      # Make sure poly_ID isn't factor
      {{ poly_ID }} := as.character({{ poly_ID }}),
      poly_area = st_area(.)
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


  ### SPLIT DATA FOR PROCESSING ################################################

  helper_progress_message("Splitting data for processing.")

  data_list <-
    intersects %>%
    group_split(.data$.point_ID) %>%
    helper_table_split()


  ### DO INTEGRATION ###########################################################

  helper_progress_message("Beginning analysis, using {helper_plan()}.")

  intersects <-
    data_list %>%
    future_map(raffle_integrate,
               # Suppress progress bar if quiet == TRUE or the plan is remote
               .progress = helper_progress(quiet)
               ) %>%
    do.call(rbind, .)

  ### PROCESS RESULTS AND RETURN OUTPUT ########################################

  # Initialize results object
  results <-
    intersects %>%
    st_drop_geometry() %>%
    group_by(.data$.point_ID)

  # Choose winners and add diagnostic field
  results <-
    results %>%
    nest(data = c({{ poly_ID }}, .data$probability)) %>%
    select(.data$.point_ID, .data$data) %>%
    summarize(
      candidates = list(bind_rows(.data$data)),
      {{ poly_ID}} := as.character(
        base::sample(map(.data$data, pull, 1), 1,
                     prob = map(.data$data, pull, 2)))
      ) %>%
    mutate(candidates = map(.data$candidates, ~{
      .x %>% mutate(probability = .data$probability / sum(.data$probability))
      }))

  # Drop diagnostic field if not requested
  if (!diagnostic) {
    results <- results %>%
      select(-.data$candidates)
  }

  # Join winners to point file and arrange output
  points <-
    points %>%
    left_join(results, by = ".point_ID") %>%
    select(-.data$.point_ID, -.data$.point_x, -.data$.point_y) %>%
    select(-.data$geometry, everything(), .data$geometry)

  helper_progress_message("Analysis complete.", .final = TRUE)

  return(points)
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
#' @importFrom polyCub polyCub.midpoint
#' @importFrom rlang .data

raffle_integrate <- function(intersects) {
  suppressMessages(
    intersects %>%
      mutate(probability = purrr::map2_dbl(.data$geometry, .data$int_units, ~{
        polyCub.midpoint(as(.x, "Spatial"), raffle_pdf) * .y
      }))
  )
}
