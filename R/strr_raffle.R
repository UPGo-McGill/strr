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


  ## Input checking ------------------------------------------------------------

  helper_check_quiet()
  stopifnot(inherits(property, "data.frame"), distance > 0, pdf == "airbnb",
            is.logical(diagnostic))


  ## Prepare batch processing variables ----------------------------------------

  iterations <- 1


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

  if (!missing(seed)) {
    set.seed(seed)
  }


  ### SET BATCH PROCESSING STRATEGY ############################################

  complexity <- as.numeric(nrow(property)) * as.numeric(nrow(polys))

  if (complexity > 1000000000) {

    grid <- sf::st_make_grid(property,
                             n = max(4, ceiling(complexity / 500000000)))

    iterations <- length(grid)

    property <- sf::st_join(property,
                            sf::st_sf(grid_id = seq_along(grid),
                                      geometry = grid))

    helper_message("Raffling point locations in ", iterations, " batches.")

  }


  ### PREPARE PROPERTY AND POLYS TABLES ########################################

  helper_message("(1/3) Preparing tables for analysis.", .type = "open")


  ## Process property ----------------------------------------------------------

  # Transform property CRS to match polys
  if (sf::st_crs(property) != sf::st_crs(polys)) {
    property <- sf::st_transform(property, sf::st_crs(polys))
  }

  # Add .point_ID field for subsequent use
  property <- dplyr::mutate(property, .point_ID = seq_len(dplyr::n()))


  ## Process polys -------------------------------------------------------------

  # Set poly_ID_flag to avoid future name collision with poly_ID
  poly_ID_flag <-
    dplyr::case_when(
      tryCatch({dplyr::select(property, poly_ID); TRUE},
               error = function(e) FALSE)  ~ "string",
      tryCatch({dplyr::select(property, {{ poly_ID }}); TRUE},
               error = function(e) FALSE)  ~ "symbol",
      TRUE ~ "none"
    )

  # Rename property$poly_ID if it exists
  if (poly_ID_flag == "string") {
    property <- dplyr::rename(property, poly_ID_temp = .data$poly_ID)
  }

  # Rename polys fields for data.table
  polys <- dplyr::rename(polys, poly_ID = {{poly_ID}}, units = {{units}})

  # Convert to data.table and remove invalid polygons
  data.table::setDT(polys)
  polys <- polys[!sf::st_is_empty(geometry) & units > 0]

  # Check for invalid polys geometry
  polys[sf::st_is(geometry, "GEOMETRYCOLLECTION"),
        geometry := sf::st_union(sf::st_collection_extract(geometry,
                                                           "POLYGON"))]

  # Cast polys to MULTIPOLYGON for consistency
  polys$geometry <- sf::st_cast(polys$geometry, "MULTIPOLYGON")

  # Clean up polys and initialize poly_area field
  polys <- polys[, .(poly_ID = as.character(poly_ID), units,
              poly_area = sf::st_area(geometry), geometry)]

  polys <- sf::st_as_sf(polys, agr = "constant")

  helper_message(
    "(1/3) Tables prepared for analysis.", .type = "close")


  ### Store empty table for later ##############################################

  empty <- polys[0,]
  empty$.point_ID <- integer()
  empty$.point_x <- numeric()
  empty$.point_y <- numeric()
  empty <- data.table::setDT(empty[,c(5:7, 1:4)])


  ### Dispatch to main helper functions ########################################

  if (iterations == 1) {

    helper_message("(2/3) Intersecting rows, using ", helper_plan(), ".")

    handler_strr("Intersecting row")

    with_progress({
      .strr_env$pb <- progressor(steps = nrow(property))
      result <- helper_intersect(property, polys, empty, distance, quiet)
      })

    if (sum(sapply(result, nrow)) == 0) {
      stop("The input tables do not intersect.")
      }

    helper_message("(3/3) Integrating rows, using ", helper_plan(), ".")

    handler_strr("Integrating row")

    with_progress({
      .strr_env$pb <- progressor(steps = sum(sapply(result, nrow)))
      suppressMessages({result <- helper_integrate(result, pdf, quiet)})
      })

  } else {

    ## Initialize lists --------------------------------------------------------

    property_list <- par_lapply(seq_len(iterations), function(i) {
      property[property$grid_id == i,]
    })

    polys_list <- par_lapply(seq_len(iterations), function(i) {
      sf::st_filter(polys, sf::st_buffer(grid[[i]], distance))
    })

    result_list <- vector("list", iterations)


    ## Intersect each batch sequentially ---------------------------------------

    helper_message("(2/3) Intersecting rows, using ", helper_plan(), ".")

    handler_strr("Intersecting row")

    with_progress({

      .strr_env$pb <- progressor(steps = nrow(property))

      for (i in seq_len(iterations)) {

        result_list[[i]] <-
          helper_intersect(property_list[[i]], polys_list[[i]], empty, distance,
                           quiet)
        }

      })

    ## Integrate each batch sequentially ---------------------------------------

    helper_message("(3/3) Integrating rows, using ", helper_plan(), ".")

    handler_strr("Integrating row")

    with_progress({

      .strr_env$pb <- progressor(
        steps = sum(sapply(result_list, function(x) sum(sapply(x, nrow)))))

      for (i in seq_len(iterations)) {

        # Don't integrate if the results are empty
        if (sum(sapply(result_list[[i]], nrow)) == 0) {

          result_list[[i]] <-
            data.table::data.table(.point_ID = integer(), candidates = list(),
                                   poly_ID = character())

        } else {

            result_list[[i]] <- helper_integrate(result_list[[i]], pdf, quiet)

        }

      }

    })

    ## Bind batches together ---------------------------------------------------

    result <-
      data.table::setorder(data.table::rbindlist(result_list), .point_ID)

  }


  ### PROCESS RESULTS AND RETURN OUTPUT ########################################

  ## Process results -----------------------------------------------------------

  # Drop diagnostic field if not requested
  if (!diagnostic) result[, candidates := NULL]

  # Join winners to point file and arrange output
  property <- dplyr::left_join(property, result, by = ".point_ID")
  property <-
    dplyr::select(property, -.data$geometry, dplyr::everything(),
                  .data$geometry, -.data$.point_ID)


  ## Rename poly_ID field in property, but check name duplication first --------

  if (poly_ID_flag == "string") {
    property <- dplyr::rename(property,
                              poly_ID_new = poly_ID,
                              poly_ID = .data$poly_ID_temp)
  } else if (poly_ID_flag == "symbol") {
    # Append "_new" to the poly_ID argument, then rename
    new_name <- paste0(rlang::as_string(rlang::ensym(poly_ID)), "_new")
    property <- dplyr::rename(property, !! new_name := .data$poly_ID)
  } else if (poly_ID_flag == "none") {
    # Rename "poly_ID" to the argument passed to poly_ID
    property <- dplyr::rename(property, {{poly_ID}} := .data$poly_ID)
  }

  helper_message("Analysis complete.", .type = "final")

  return(property)
}


#' Helper function to intersect property and polys
#'
#' \code{helper_intersect} intersects property and polys
#'
#' @param property,polys,empty,distance,quiet Arguments passed along from the
#' main function.
#' @return The output will be a list of two data frames.

helper_intersect <- function(property, polys, empty, distance, quiet) {

  geometry <- .point_ID <- NULL

  ## Define function to be mapped ----------------------------------------------

  intersect_fun <- function(x) {

    .strr_env$pb(amount = nrow(x))

    output <- sf::st_as_sf(x, coords = c(".point_x", ".point_y"),
                           crs = sf::st_crs(property),
                           remove = FALSE, agr = "constant")

    output <- sf::st_buffer(output, distance, 10)
    output <- sf::st_intersection(output, polys)

    # Cast multipolygons to polygons
    if (nrow(output) > 0) {
      multi_polys <- dplyr::filter(output, !sf::st_is(geometry, "POLYGON"))
      multi_polys <- sf::st_cast(multi_polys, "MULTIPOLYGON", warn = FALSE)
      multi_polys <- sf::st_cast(multi_polys, "POLYGON", warn = FALSE)
      output <- rbind(multi_polys,
                      dplyr::filter(output, sf::st_is(geometry, "POLYGON")))
    }

    return(output)

  }

  ## Initialize intersects -----------------------------------------------------

  intersects <- data.table::copy(property)


  ## Prepare intersects for processing -----------------------------------------

  # Initialize helper fields and drop geometry for the split
  intersects <-
    data.table::setDT(intersects)[, .(.point_ID,
                          .point_x = sf::st_coordinates(geometry)[,1],
                          .point_y = sf::st_coordinates(geometry)[,2])]

  intersects <- helper_table_split(split(intersects, by = ".point_ID"), 100)


  ## Generate buffers and intersect with polygons ------------------------------

  intersects <- par_lapply(intersects, intersect_fun)


  ## Process results -----------------------------------------------------------

  # Recombine output list
  intersects <- intersects[sapply(intersects, nrow) > 0]

  # Exit early if no intersections are found
  if (length(intersects) == 0) return(list(empty, empty))

  # Convert back to sf
  intersects <- sf::st_as_sf(data.table::rbindlist(intersects))

  # Store results where there is only one possible option
  one_choice <- setDT(intersects)[, if (.N == 1) .SD, by = ".point_ID"]
  if (nrow(one_choice) == 0) one_choice <- empty

  intersects <- intersects[, if (.N > 1) .SD, by = ".point_ID"]
  if (nrow(intersects) == 0) intersects <- empty

  return(list(one_choice, intersects))
}


#' Helper function to integrate intersects
#'
#' \code{helper_integrate} integrates intersects
#'
#' @param result,pdf,quiet Arguments passed along from the main function.
#' @return The output will be a data frame.

helper_integrate <- function(result, pdf, quiet) {

  poly_ID <- .point_ID <- geometry <- poly_area <- .point_x <- .point_y <-
    probability <- NULL

  one_choice <- result[[1]]
  intersects <- result[[2]]

  ## Define function to be mapped ----------------------------------------------

  integrate_fun <- function(.x) {
    .strr_env$pb(amount = nrow(.x))
    helper_raffle_integrate(data.table::setDT(.x))
  }


  ## Exit early if there are no points with multiple intersects ----------------

  if (nrow(intersects) == 0) {

    one_choice <-
      one_choice[, .(candidates = list(data.table(poly_ID, probability = 1)),
                     poly_ID), by = .point_ID]

    result <- one_choice


  ## Otherwise proceed with integration ----------------------------------------

  } else {

    # Estimate int_units, transform intersects relative to point coordinates
    coord_shift <- function(g, x, y) g - c(x, y)

    intersects[, c("int_units", "geometry", ".PID_split") :=
                 list(as.numeric(units * sf::st_area(geometry) / poly_area),
                      sf::st_sfc(mapply(coord_shift, geometry, .point_x,
                                        .point_y, SIMPLIFY = FALSE)),
                      substr(.point_ID, 1, 3))]

    # Split data for processing
    data_list <- split(intersects, by = ".PID_split", keep.by = FALSE)
    data_list <- helper_table_split(data_list, 100)

    # Do integration
    intersects <- par_lapply(data_list, integrate_fun)
    intersects <- data.table::rbindlist(intersects)

    # Produce result object
    result <-
      intersects[, .(
        candidates = list(data.table(poly_ID, probability)),
        poly_ID = base::sample(poly_ID, size = 1, prob = probability)),
        keyby = .point_ID]

    # Add result from one_choice
    if (nrow(one_choice) > 0) {
      one_choice <-
        one_choice[, .(candidates = list(
          data.table(poly_ID, probability = 1)), poly_ID), by = .point_ID]

      result <- data.table::setorder(
        data.table::rbindlist(list(result, one_choice)), .point_ID)
    }
  }


  ## Return output -------------------------------------------------------------

  return(result)

}


#' Helper function specifying the probability density function
#'
#' \code{raffle_pdf_airbnb} specifies the probability density function which
#' Airbnb uses to obfuscate listing locations.
#'
#' A function which specifies the probability density function which Airbnb
#' uses to obfuscate listing locations (a radially symmetric normal distribution
#' with mean 100 m and standard deviation 50 m, truncated at r = 0 and r = 200).
#' In the future, other PDFs may be added.
#'
#' @param x An st_coordinates object, converted to sp.
#' @return The output will be a numerical vector of probabilities.

raffle_pdf_airbnb <- function(x) {
  stats::dnorm(sqrt(x[,1]^2 + x[,2]^2), mean = 100, sd = 50, log = FALSE) *
    (1 / (2 * pi))
}


#' Helper function to integrate raffle_pdf_* over intersect polygons
#'
#' \code{helper_raffle_integrate} takes raffle_pdf_* and integrates it over the
#' intersect polygons using polyCub.
#'
#' A function for integrating raffle_pdf_* over the intersect polygons using
#' polyCub. In the future, other PDFs may be added.
#'
#' @param intersects A data.table with an sf geometry column.
#' @return The output will be the input data.table with a `probability` field
#' added.

helper_raffle_integrate <- function(intersects) {

  suppressMessages({

    probability <- geometry <- int_units <- NULL

    int_fun <- function(x, y) {
      polyCub::polyCub.midpoint(x, raffle_pdf_airbnb) * y
    }

    intersects[, probability := mapply(int_fun, geometry, int_units)]

  })
}
