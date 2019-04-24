#' Raffle component functions


## Point setup function, returns points ----------------------------------------

raffle_setup_points <- function(points, point_ID) {

  points <-
    points %>%
    filter(!! point_ID > 0) %>%
    arrange(!! point_ID)

  points
}


## Polygon setup function, returns polys ---------------------------------------

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


## Intersect point buffers with polygons, returns intersects -------------------

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


## PDF helper function, returns vector of probabilities ------------------------

raffle_pdf <- function(x) {
  dnorm(sqrt(x[,1]^2 + x[,2]^2), mean = 100, sd = 50, log = FALSE) *
    (1 / (2 * pi))
}

## Integrate the PDF over intersect polygons, returns intersects ---------------

raffle_integrate <- function(intersects) {

  intersects <-
    intersects %>%
    mutate(probability = map2_dbl(geometry, int_units, ~{
      polyCub.midpoint(as(.x, "Spatial"), raffle_pdf) * .y
    })
    )

  intersects
}


## Determine winners, returns points -------------------------------------------

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
