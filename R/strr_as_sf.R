#' Function to convert property tables to sf objects
#'
#' \code{strr_as_sf} is a convenience wrapper around \code{sf::st_as_sf} for STR
#' property tables.
#'
#' A function for converting a raw property table into an sf object with point
#' geometries. The function makes use of a pair of fields specifying longitude
#' and latitude, and has the option to specify a destination CRS.
#'
#' @param .data A property table in standard UPGo format.
#' @param CRS The EPSG code or proj4string character vector specifying a
#' coordinate reference system into which to transform the table. The default
#' is unprojected WGS 84 (EPSG 4326).
#' @param longitude A character string naming the field supplying the
#' longitude values of points.
#' @param latitude A character string naming the field supplying the
#' latitude values of points.
#' @return An sf table with the longitude and latitude fields converted to a
#' geometry field and all other fields returned unaltered.
#' @importFrom sf st_as_sf
#' @export

strr_as_sf <- function(.data, CRS = 4326, longitude = "longitude",
                       latitude = "latitude") {

  st_transform(st_as_sf(.data, coords = c(longitude, latitude), crs = 4326),
               CRS)

}
