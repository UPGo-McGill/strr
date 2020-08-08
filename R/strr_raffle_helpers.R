#' Helper function to prepare property
#'
#' @param property,polys,poly_ID_flag Arguments passed along from the main
#' function.
#' @return The output will be the `property` table.

helper_prepare_property <- function(property, polys, poly_ID_flag) {

  # Transform property CRS to match polys
  if (sf::st_crs(property) != sf::st_crs(polys)) {
    property <- sf::st_transform(property, sf::st_crs(polys))
  }

  # Add .point_ID field for subsequent use
  property <- dplyr::mutate(property, .point_ID = seq_len(dplyr::n()))


  # Rename property$poly_ID if it exists
  if (poly_ID_flag == "string") {
    property <- dplyr::rename(property, poly_ID_temp = .data$poly_ID)
  }

  return(property)

}


#' Helper function to prepare polys
#'
#' @param property,polys,poly_ID_flag Arguments passed along from the main
#' function.
#' @return The output will be the `polys` table.

helper_prepare_polys <- function(property, polys, poly_ID_flag) {

  geometry <- poly_ID <- NULL

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

  return(polys)

}

#' Helper function to prepare grid
#'
#' @param property,polys,distance Arguments passed along from the main
#' function.
#' @return The output will be a list with the `property` table, the grid
#' geometry, and the number of iterations.

helper_prepare_grid <- function(property, polys, distance) {

  grid <- n <- NULL

  ## Get iterations ------------------------------------------------------------

  complexity <-
    as.integer(ceiling((distance ^ 2 * pi) / mean(sf::st_area(polys)) / 5))
  target_rows_per_grid <- max(ceiling(10000 / complexity), 100)
  iterations <- ceiling(nrow(property) / target_rows_per_grid)

  if (iterations > 1) {

    ## Make first grid ---------------------------------------------------------

    grid <- sf::st_make_grid(property, n = 2)
    grid_table <- sf::st_sf(.grid_ID = seq_along(grid), geometry = grid)
    property$.grid_ID <- NULL
    property <- sf::st_join(property, grid_table)
    grid_count <- dplyr::count(sf::st_drop_geometry(property), .data$.grid_ID)
    grid_table <- dplyr::left_join(grid_table, grid_count, by = ".grid_ID")

    ## Consolidate if necessary ------------------------------------------------

    grid_table <- dplyr::arrange(grid_table, .data$n)

    while (sum(grid_table$n[1:2]) < target_rows_per_grid) {

      grid_table$merge <- c(1, 1, 2:(nrow(grid_table) - 1))
      grid_table <- dplyr::group_by(grid_table, merge)
      grid_table <- dplyr::summarize(grid_table, n = sum(n))

    }

    grid_table$merge <- NULL
    grid_table$.grid_ID <- seq_len(nrow(grid_table))
    grid_table <- grid_table[c(".grid_ID", "n", "geometry")]
    grid <- grid_table$geometry


    ## Make subsequent grids ---------------------------------------------------

    while (any(grid_table$n > target_rows_per_grid)) {

      new_grid <- grid[grid_table$n > target_rows_per_grid]
      new_grid <- lapply(new_grid, sf::st_make_grid, n = 2)
      new_grid <-
        lapply(new_grid, function(x) {
        new_grid_table <- sf::st_sf(.grid_ID = seq_along(x), geometry = x,
                                crs = sf::st_crs(grid_table))
        prop <- sf::st_filter(property, new_grid_table)
        prop$.grid_ID <- NULL
        prop <- sf::st_join(prop, new_grid_table)
        new_grid_count <-
          dplyr::count(sf::st_drop_geometry(prop), .data$.grid_ID)
        new_grid_table <- dplyr::left_join(new_grid_table, new_grid_count,
                                           by = ".grid_ID")
        new_grid_table <- dplyr::filter(new_grid_table, !is.na(.data$n))
        new_grid_table <- dplyr::arrange(new_grid_table, .data$n)

        while (sum(new_grid_table$n[1:2]) < target_rows_per_grid) {

          new_grid_table$merge <- c(1, 1, 2:(nrow(new_grid_table) - 1))
          new_grid_table <- dplyr::group_by(new_grid_table, merge)
          new_grid_table <- dplyr::summarize(new_grid_table, n = sum(n))

        }

        new_grid_table$merge <- NULL
        new_grid_table$.grid_ID <- seq_len(nrow(new_grid_table))
        new_grid_table <- new_grid_table[c(".grid_ID", "n", "geometry")]

        new_grid_table$geometry
      })

      new_grid <- unlist(new_grid, recursive = FALSE)
      new_grid <- sf::st_as_sfc(new_grid)
      new_grid <- c(new_grid, grid[grid_table$n <= target_rows_per_grid])
      new_grid <- sf::st_set_crs(new_grid, sf::st_crs(property))

      grid_table <- sf::st_sf(.grid_ID = seq_along(new_grid),
                              geometry = new_grid)
      property$.grid_ID <- NULL
      property <- sf::st_join(property, grid_table)
      grid_count <- dplyr::count(sf::st_drop_geometry(property), .data$.grid_ID)
      grid_table <- dplyr::left_join(grid_table, grid_count, by = ".grid_ID")
      grid_table <- dplyr::filter(grid_table, .data$n > 0)
      grid_table$.grid_ID <- seq_along(grid_table$.grid_ID)
      grid <- grid_table$geometry
      iterations <- length(grid)

    }

  }

  return(list(property, grid, iterations))
}


#' Helper function to prepare to intersect property and polys
#'
#' @param property Argument passed along from the main function.

helper_prepare_intersect <- function(property) {

  geometry <- .point_ID <- NULL

  # Initialize data_list
  data_list <- data.table::copy(property)

  # Initialize helper fields and drop geometry for the split
  data_list <- data.table::setDT(
    data_list)[, .(.point_ID,
                   .point_x = sf::st_coordinates(geometry)[,1],
                   .point_y = sf::st_coordinates(geometry)[,2])]

  # Split data
  data_list <- helper_table_split(split(data_list, by = ".point_ID"), 100)

  return(data_list)

}

#' Helper function to intersect property and polys
#'
#' @param x,polys,distance Arguments passed along from the main function.

helper_intersect <- function(x, polys, distance) {

  geometry <- NULL

  output <- sf::st_as_sf(x, coords = c(".point_x", ".point_y"),
                         crs = sf::st_crs(polys),
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

#' Helper function to process intersections of property and polys
#'
#' @param intersects,empty Arguments passed along from the main function.

helper_process_intersects <- function(intersects, empty) {

  poly_ID <- probability <- .point_ID <- NULL

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


#' Helper function to prepare to integrate intersects
#'
#' @param intersects Argument passed along from the main function.

helper_prepare_integrate <- function(intersects) {

  geometry <- poly_area <- .point_x <- .point_y <- .point_ID <- NULL

  intersects <- intersects[[2]]

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

  return(data_list)

}


#' Helper function to integrate raffle_pdf_* over intersect polygons
#'
#' @param x,y,pdf Arguments passed along from main function.

helper_integrate <- function(x, y, pdf) {

  polyCub::polyCub.midpoint(x, pdf) * y

}


helper_process_integrate <- function(result, intersects) {

  poly_ID <- probability <- .point_ID <- NULL

  one_choice <- intersects[[1]]
  result <- data.table::rbindlist(result)

  # Produce result object
  result <-
    result[, .(candidates = list(data.table(poly_ID, probability)),
               poly_ID = base::sample(poly_ID, size = 1, prob = probability)),
           keyby = .point_ID]

  # Add result from one_choice
  if (nrow(one_choice) > 0) {
    one_choice <- one_choice[, .(candidates = list(
      data.table(poly_ID, probability = 1)), poly_ID), by = .point_ID]

    result <- data.table::setorder(
      data.table::rbindlist(list(result, one_choice)), .point_ID)
    }

  # Return output
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


#' Helper function to process raffle results
#'
#' @param property,result,diagnostic,poly_ID_flag,poly_ID_name Arguments passed
#' along from the main function.
#' @return The output will be the `property` table.

helper_process_results <- function(property, result, diagnostic, poly_ID_flag,
                                   poly_ID_name) {

  candidates <- poly_ID <- NULL

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
    new_name <- paste0(rlang::as_string(poly_ID_name), "_new")
    property <- dplyr::rename(property, !! new_name := .data$poly_ID)
  } else if (poly_ID_flag == "none") {
    # Rename "poly_ID" to the argument passed to poly_ID
    property <- dplyr::rename(property, !! poly_ID_name := .data$poly_ID)
  }

  return(property)
}
