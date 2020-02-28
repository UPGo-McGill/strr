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
#' @param seed An integer scalar. A seed for the random number generation, to
#' allow reproducible results between iterations of strr_raffle. If NULL
#' (default), a new seed will be chosen each time.
#' @param pdf A character scalar. The probability density function which should
#' be used for assigning listing locations within the `distance` radius. The
#' only option currently is "airbnb", which is a radially symmetric normal
#' curve around the point origin, with mean 100 m and standard deviation 50 m.
#' More options may be added in the future.
#' @param diagnostic A logical scalar. Should a list of polygon candidates and
#'   associated probabilities be appended to the function output?
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be the input points object with a new field
#'   appended, which specifies which polygon from the `polys` object was
#'   probabilistically assigned to the listing, and which takes the name of the
#'   field identified in the `poly_ID` argument. (If there was already a field
#'   with that name in the `points` object, the new field will have "_new"
#'   appended to its name.) If diagnostic == TRUE, a `candidates` field will
#'   also be appended, which lists the possible polygons for each point, along
#'   with their probabilities.
#' @importFrom data.table copy data.table setDT setorder
#' @importFrom dplyr %>% everything left_join mutate n select
#' @importFrom rlang := .data
#' @importFrom sf st_area st_as_sf st_buffer st_cast st_coordinates st_crs
#' @importFrom sf st_collection_extract st_intersection st_sfc st_transform
#' @export

strr_raffle <- function(
  points, polys, poly_ID, units, distance = 200, seed = NULL, pdf = "airbnb",
  diagnostic = FALSE, quiet = FALSE) {

  time_1 <- Sys.time()


  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  helper_progress_message("Raffling point locations.")

  ## Prepare data.table variables

  .datatable.aware = TRUE

  .I <- .point_ID <- .point_x <- .point_y <- candidates <- geometry <-
    int_units <- poly_area <- probability <- NULL

  # Suppress message about spatstat s3 message
  s3_warn <- Sys.getenv("_R_S3_METHOD_REGISTRATION_NOTE_OVERWRITES_")
  Sys.setenv("_R_S3_METHOD_REGISTRATION_NOTE_OVERWRITES_" = FALSE)

  # Remove future global export limit
  options(future.globals.maxSize = +Inf)

  # Print \n on exit so error messages don't collide with progress messages
  # And restore s3 overwrite setting
  # And remove future global size setting
  on.exit({
    if (!quiet) message()
    Sys.setenv("_R_S3_METHOD_REGISTRATION_NOTE_OVERWRITES_" = s3_warn)
    .Options$future.globals.maxSize <- NULL
    })

  ## Check distance, pdf, diagnostic and quiet flags

  # Check that distance > 0
  if (distance <= 0) {
    stop("The argument `distance` must be a positive number.")
  }

  # Check that pdf == "airbnb"

  if (!pdf == "airbnb") {
    stop("The argument `pdf` must take the value 'airbnb'.")
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
      helper_progress_message("Input table converted to sf.")
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


  ## Set poly_ID_flag to avoid future name collision with poly_ID

  poly_ID_flag <-
    dplyr::case_when(
      tryCatch({select(points, poly_ID); TRUE},
               error = function(e) FALSE)  ~ "string",
      tryCatch({select(points, {{ poly_ID }}); TRUE},
               error = function(e) FALSE)  ~ "symbol",
      TRUE ~ "none"
    )

  # Rename points$poly_ID if it exists
  if (poly_ID_flag == "string") {
    points <- rename(points, poly_ID_temp = .data$poly_ID)
  }

  ## Set seed if seed is supplied

  if (!missing(seed)) {
    set.seed(seed)
  }


  ### PREPARE POINTS AND POLYS TABLES ##########################################

  helper_progress_message("(1/4) Preparing tables for analysis.",
                          .type = "open")

  # Transform polys CRS to match points and rename fields for data.table
  polys <- st_transform(polys, st_crs(points)) %>%
    rename(poly_ID = {{ poly_ID }},
           units = {{ units }})

  # Clean up polys and initialize poly_area field
  polys <-
    setDT(polys)[units > 0, .(poly_ID = as.character(poly_ID), units,
                              poly_area = st_area(geometry),
                              geometry)] %>%
    st_as_sf(agr = "constant")

  # Initialize intersects
  points <- mutate(points, .point_ID = seq_len(n()))
  intersects <- copy(points)

  helper_progress_message("(1/4) Tables prepared for analysis.",
                          .type = "close")

  helper_progress_message("(2/4) Intersecting points with polygons.",
                          .type = "open")

  # Generate buffers and intersect with polygons
  intersects <-
    # Initialize helper fields
    setDT(intersects)[, .(
      .point_ID,
      .point_x = st_coordinates(geometry)[,1],
      .point_y = st_coordinates(geometry)[,2],
      geometry = st_buffer(geometry, dist = distance, nQuadSegs = 10))] %>%
    st_as_sf(agr = "constant") %>%
    st_intersection(polys)

  # Cast multipolygons to polygons
  intersects <-
    intersects %>%
    filter(!st_is(geometry, "POLYGON")) %>%
    st_cast("POLYGON", warn = FALSE) %>%
    rbind(filter(intersects, st_is(geometry, "POLYGON")))

  # Store results where there is only one possible option
  one_choice <- setDT(intersects)[, if (.N == 1) .SD, by = ".point_ID"]

  intersects <- intersects[, if (.N > 1) .SD, by = ".point_ID"]

  # Estimate int_units and transform intersects relative to point coordinates
  coord_shift <- function(g, x, y) g - c(x, y)

  intersects[, c("int_units", "geometry", ".PID_split") :=
               list(as.numeric(units * st_area(geometry) / poly_area),
                    st_sfc(mapply(coord_shift, geometry, .point_x,
                                  .point_y, SIMPLIFY = FALSE)),
                    substr(.point_ID, 1, 3))]

  helper_progress_message("(2/4) Points intersected with polygons.",
                          .type = "close")


  ### SPLIT DATA FOR PROCESSING ################################################

  helper_progress_message("(3/4) Splitting data for processing.",
                          .type = "open")

  data_list <-
    split(intersects, by = ".PID_split", keep.by = FALSE) %>%
    helper_table_split()

  helper_progress_message("(3/4) Data split for processing.", .type = "close")


  ### DO INTEGRATION ###########################################################

  helper_progress_message("(4/4) Beginning analysis, using {helper_plan()}.",
                          .type = "progress")

  intersects <-
    data_list %>%
    map(setDT) %>%
    future_map(raffle_integrate, .progress = helper_progress()) %>%
    rbindlist()

  ### PROCESS RESULTS AND RETURN OUTPUT ########################################

  # Produce results object
  results <-
    intersects[, .(
      candidates = list(data.table(poly_ID, probability)),
      poly_ID = base::sample(poly_ID, size = 1, prob = probability)),
      keyby = .point_ID]

  # Add results from one_choice
  if (nrow(one_choice) > 0) {
    one_choice <-
      one_choice[, .(candidates = list(data.table(poly_ID, probability = 1)),
                     poly_ID),
                 by = .point_ID]

    results <-
      setorder(rbindlist(list(results, one_choice)), .point_ID)
  }

  # Drop diagnostic field if not requested
  if (!diagnostic) results[, candidates := NULL]

  # Join winners to point file and arrange output
  points <-
    points %>%
    left_join(results, by = ".point_ID") %>%
    select(-.data$.point_ID) %>%
    select(-.data$geometry, everything(), .data$geometry)

  # Rename poly_ID field in points, but check name duplication first

  if (poly_ID_flag == "string") {
    points <-
      points %>% rename(poly_ID_new = poly_ID, poly_ID = .data$poly_ID_temp)
  } else if (poly_ID_flag == "symbol") {
    # Append "_new" to the poly_ID argument, then rename
    new_name <- paste0(rlang::as_string(ensym(poly_ID)), "_new")
    points <- points %>% rename(!! new_name := .data$poly_ID)
  } else if (poly_ID_flag == "none") {
    # Rename "poly_ID" to the argument passed to poly_ID
    points <- points %>% rename({{ poly_ID }} := .data$poly_ID)
  }

  helper_progress_message("Analysis complete.", .type = "final")

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
#' @param intersects A data.table with an sf geometry column.
#' @return The output will be the input data frame with a `probability` field
#' added.
#' @importFrom methods as
#' @importFrom polyCub polyCub.midpoint
#' @importFrom rlang .data

raffle_integrate <- function(intersects) {

  .datatable.aware = TRUE
  probability <- geometry <- int_units <- NULL

  int_fun <- function(x, y) {
    polyCub.midpoint(as(x, "Spatial"), raffle_pdf) * y
  }

  intersects[, probability := mapply(int_fun, geometry, int_units)]
  }
