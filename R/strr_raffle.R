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
#' @param property An sf or sp point-geometry object, in a projected coordinate
#' system. If the data frame does not have spatial attributes, an attempt will
#' be made to convert it to sf using \code{\link{strr_as_sf}}. The result will
#' be transformed into the CRS of `polys`.
#' @param polys An sf or sp polygon-geometry object with administrative
#' geographies as polygons.
#' @param poly_ID The name of a character or numeric variable in the polys
#' object to be used as an ID to identify the "winning" polygon assigned to
#' each point in the output.
#' @param units The name of a numeric variable in the polys object which
#' contains the weighting factor (number of people or housing units).
#' @param distance A numeric scalar. The radius (in the units of the CRS) of the
#' buffer which will be drawn around points to determine possible listing
#' locations.
#' @param seed An integer scalar. A seed for the random number generation, to
#' allow reproducible results between iterations of strr_raffle. If NULL
#' (default), a new seed will be chosen each time.
#' @param pdf A character scalar. The probability density function which should
#' be used for assigning listing locations within the `distance` radius. The
#' only option currently is "airbnb", which is a radially symmetric normal
#' curve around the point origin, with mean 100 m and standard deviation 50 m.
#' More options may be added in the future.
#' @param diagnostic A logical scalar. Should a list of polygon candidates and
#' associated probabilities be appended to the function output?
#' @param quiet A logical scalar. Should the function execute quietly, or should
#' it return status updates throughout the function (default)?
#' @return The output will be the input property object with a new field
#' appended, which specifies which polygon from the `polys` object was
#' probabilistically assigned to the listing, and which takes the name of the
#' field identified in the `poly_ID` argument. (If there was already a field
#' with that name in the `property` object, the new field will have "_new"
#' appended to its name.) If diagnostic == TRUE, a `candidates` field will
#' also be appended, which lists the possible polygons for each point, along
#' with their probabilities.
#' @importFrom data.table copy data.table setDT setorder
#' @importFrom dplyr %>%
#' @importFrom rlang := .data
#' @export

strr_raffle <- function(
  property, polys, poly_ID, units, distance = 200, seed = NULL, pdf = "airbnb",
  diagnostic = FALSE, quiet = FALSE) {

  ### ERROR CHECKING AND ARGUMENT INITIALIZATION ###############################

  start_time <- Sys.time()
  poly_ID_name <- rlang::ensym(poly_ID)


  ## Input checking ------------------------------------------------------------

  helper_check_quiet()
  stopifnot(inherits(property, "data.frame"), distance > 0, pdf == "airbnb",
            is.logical(diagnostic))


  ## Prepare data.table variables ----------------------------------------------

  .point_ID <- .point_x <- .point_y <- candidates <- geometry <- int_units <-
    poly_area <- probability <- NULL


  ## Handle spatial attributes -------------------------------------------------

  # Convert property and polys from sp
  if (inherits(property, "Spatial")) {
    property <- sf::st_as_sf(property)
  }
  if (inherits(polys, "Spatial")) {
    polys <- sf::st_as_sf(polys)
  }

  # Check that property is sf, and convert to sf if possible
  if (!inherits(property, "sf")) {
    tryCatch({
      property <- strr_as_sf(property, sf::st_crs(polys))
      helper_message("Input table converted to sf.")
      },
    error = function(e) {
      stop(paste0("The object `property` must be of class sf or sp, ",
                  "or must be convertable to sf using strr_as_sf."))
    })
  }

  # Check that property and polys are sf
  if (inherits(property, "sf") == FALSE) {
    stop(paste0("The object `property` must be of class sf or sp, ",
                "or must be convertable to sf using strr_as_sf."))
  }
  if (inherits(polys, "sf") == FALSE) {
    stop("The object `polys` must be of class sf or sp.")
  }


  ## Check that polys fields exist ---------------------------------------------

  tryCatch(
    dplyr::pull(polys, {{poly_ID}}),
    error = function(e) stop(
      "The value of `poly_ID` is not a valid field in the `polys` input table."
      ))

  tryCatch(
    dplyr::pull(polys, {{units}}),
    error = function(e) stop(
      "The value of `units` is not a valid field in the `polys` input table."
      ))


  ## Set seed if seed is supplied ----------------------------------------------

  if (!missing(seed)) set.seed(seed)


  ### PREPARE PROPERTY AND POLYS TABLES ########################################

  helper_message("(1/3) Preparing tables for analysis.", .type = "open")

  # Set poly_ID_flag to avoid future name collision with poly_ID
  poly_ID_flag <-
    dplyr::case_when(tryCatch({dplyr::select(property, poly_ID); TRUE},
                              error = function(e) FALSE)  ~ "string",
                     tryCatch({dplyr::select(property, {{poly_ID}}); TRUE},
                              error = function(e) FALSE)  ~ "symbol",
                     TRUE ~ "none")

  # Rename polys fields for data.table
  polys <- sf::st_set_agr(polys, "constant") # Avoids rename.sf error
  polys <- dplyr::rename(polys, poly_ID = {{poly_ID}}, units = {{units}})


  ## Process property and polys ------------------------------------------------

  property <- helper_prepare_property(property, polys, poly_ID_flag)
  polys <- helper_prepare_polys(property, polys, poly_ID_flag)


  ### Store empty table for later ##############################################

  empty <- data.table::setDT(polys[0,])[
    , c(".point_ID", ".point_x", ".point_y") := .(integer(), numeric(),
                                                  numeric())][,c(5:7, 1:4)]


  ### SET BATCH PROCESSING STRATEGY ############################################

  inputs <- helper_prepare_grid(property, polys, distance)

  property <- inputs[[1]]
  grid <- inputs[[2]]
  iterations <- inputs[[3]]

  helper_message("(1/3) Tables prepared for analysis.", .type = "close")


  ### SINGLE ITERATION #########################################################

  if (iterations == 1) {

    ## Intersect rows ----------------------------------------------------------

    helper_message("(2/3) Intersecting rows, using ", helper_plan(), ".")

    data_list <- helper_prepare_intersect(property)

    handler_strr("Intersecting row")

    with_progress({
      pb <- progressor(steps = nrow(property))
      intersects <- par_lapply(data_list, function(x) {
        pb(amount = nrow(x))
        helper_intersect(x, polys, distance)
        })
      })

    intersects <- helper_process_intersects(intersects, empty)

    if (sum(sapply(intersects, nrow)) == 0) {
      stop("The input tables do not intersect.")}


    ## Integrate rows ----------------------------------------------------------

    helper_message("(3/3) Integrating rows, using ", helper_plan(), ".")

    # Exit early if only one_choice
    if (nrow(intersects[[2]]) == 0) {

      result <- intersects[[1]]

      property <- helper_process_results(property, result, diagnostic,
                                         poly_ID_flag, poly_ID_name)

      helper_message("Analysis complete.", .type = "final")

      return(property)

    }

    # Otherwise proceed to integration
    data_list <- helper_prepare_integrate(intersects)

    handler_strr("Integrating row")

    with_progress({

      pb <- progressor(steps = sum(sapply(data_list, nrow)))

      result <- par_lapply(data_list, function(x, pdf) {

        pb(amount = nrow(x))

        x[, probability := mapply(helper_integrate, geometry, int_units,
                                  MoreArgs = list(pdf))]

        }, raffle_pdf_airbnb)
      })

    result <- helper_process_integrate(result, intersects)

    property <- helper_process_results(property, result, diagnostic,
                                       poly_ID_flag, poly_ID_name)

    helper_message("Analysis complete.", .type = "final")

    return(property)

  }


  ### INITIALIZE LISTS #########################################################

  helper_message("(2/3) Intersecting rows in ", iterations,
                 " batches using ", helper_plan(), ".")

  # handler_strr("Preparing points: batch")

  # with_progress({

    # pb <- progressor(steps = iterations)

    property_list <- lapply(seq_len(iterations), function(i) {
      # pb()
      property[property$.grid_ID == i,]
    })

  # })

  # handler_strr("Preparing polygons: batch")

  # with_progress({

    # pb <- progressor(steps = iterations)

    polys_list <- par_lapply(seq_len(iterations), function(i) {
      # pb()
      sf::st_filter(polys, sf::st_buffer(grid[[i]], distance))
    })

  # })

  intersects_list <- vector("list", iterations)
  result_list <- vector("list", iterations)


  ### INTERSECT BATCHES ########################################################

  handler_strr("Intersecting row")

  with_progress({

    pb <- progressor(steps = nrow(property))

    for (i in seq_len(iterations)) {

      data_list <- helper_prepare_intersect(property_list[[i]])

      intersects <- par_lapply(data_list, function(x) {
        pb(amount = nrow(x))
        helper_intersect(x, polys_list[[i]], distance)
        })

      intersects_list[[i]] <- helper_process_intersects(intersects, empty)

    }

  })


  ### INTEGRATE BATCHES ########################################################

  helper_message("(3/3) Integrating rows, using ", helper_plan(), ".")

  # handler_strr("Integrating row")

  # with_progress({

    # pb <- progressor(steps = sum(sapply(lapply(intersects_list, `[[`, 2),
                                        # nrow)))

    for (i in seq_len(iterations)) {

      intersects <- intersects_list[[i]]

      ## Exit early if only one_choice -----------------------------------------

      if (nrow(intersects[[2]]) == 0) {

        result_list[[i]] <- intersects[[1]][, .(candidates = list(
          data.table(poly_ID, probability = 1)), poly_ID), by = .point_ID]

        next

      }

      ## Otherwise proceed to integration --------------------------------------

      data_list <- helper_prepare_integrate(intersects)

      result <- par_lapply(data_list, function(x, pdf) {

        # pb(amount = nrow(x))

        x[, probability := mapply(helper_integrate, geometry, int_units,
                                  MoreArgs = list(pdf))]

      }, raffle_pdf_airbnb)

      result_list[[i]] <- helper_process_integrate(result, intersects)

    }

  # })

  ### BIND BATCHES TOGETHER ####################################################

  result <- data.table::setorder(data.table::rbindlist(result_list), .point_ID)


  ### PROCESS RESULTS AND RETURN OUTPUT ########################################

  property <- helper_process_results(property, result, diagnostic, poly_ID_flag,
                                     poly_ID_name)

  helper_message("Analysis complete.", .type = "final")

  return(property)
}
