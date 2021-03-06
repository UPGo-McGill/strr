% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/strr_raffle.R
\name{strr_raffle}
\alias{strr_raffle}
\title{Raffle to assign STR listings to administrative units for spatial analysis}
\usage{
strr_raffle(
  property,
  polys,
  poly_ID,
  units,
  distance = 200,
  seed = NULL,
  pdf = "airbnb",
  diagnostic = FALSE,
  quiet = FALSE
)
}
\arguments{
\item{property}{An sf or sp point-geometry object, in a projected coordinate
system. If the data frame does not have spatial attributes, an attempt will
be made to convert it to sf using \code{\link{strr_as_sf}}. The result will
be transformed into the CRS of `polys`.}

\item{polys}{An sf or sp polygon-geometry object with administrative
geographies as polygons.}

\item{poly_ID}{The name of a character or numeric variable in the polys
object to be used as an ID to identify the "winning" polygon assigned to
each point in the output.}

\item{units}{The name of a numeric variable in the polys object which
contains the weighting factor (number of people or housing units).}

\item{distance}{A numeric scalar. The radius (in the units of the CRS) of the
buffer which will be drawn around points to determine possible listing
locations.}

\item{seed}{An integer scalar. A seed for the random number generation, to
allow reproducible results between iterations of strr_raffle. If NULL
(default), a new seed will be chosen each time.}

\item{pdf}{A character scalar. The probability density function which should
be used for assigning listing locations within the `distance` radius. The
only option currently is "airbnb", which is a radially symmetric normal
curve around the point origin, with mean 100 m and standard deviation 50 m.
More options may be added in the future.}

\item{diagnostic}{A logical scalar. Should a list of polygon candidates and
associated probabilities be appended to the function output?}

\item{quiet}{A logical scalar. Should the function execute quietly, or should
it return status updates throughout the function (default)?}
}
\value{
The output will be the input property object with a new field
appended, which specifies which polygon from the `polys` object was
probabilistically assigned to the listing, and which takes the name of the
field identified in the `poly_ID` argument. (If there was already a field
with that name in the `property` object, the new field will have "_new"
appended to its name.) If diagnostic == TRUE, a `candidates` field will
also be appended, which lists the possible polygons for each point, along
with their probabilities.
}
\description{
\code{strr_raffle} takes reported STR listing locations and assigns the
listings to administrative units based on a probability density function and
other information about population or housing distribution.
}
\details{
A function for probablistically assigning STR listings to administrative
geographies (e.g. census tracts) based on reported latitude/longitude.
The function works by combining a known probability density function (e.g.
Airbnb's spatial obfuscation of listing locations) with an additional source
of information about possible listing locations--either population or housing
densities.
}
