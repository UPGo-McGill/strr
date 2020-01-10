### Code used to generate sample dataset for testing strr_raffle ###############

library(tidyverse)
library(sf)
library(upgo)
library(strr)
library(cancensus)

upgo_connect()

points <-
  property_all %>%
  filter(country == "Canada", city == "Charlottetown",
         scraped <= "2019-11-30") %>%
  collect() %>%
  filter(!is.na(listing_type)) %>%
  strr_as_sf(32620)

polys <-
  get_census(
    dataset = "CA16", regions = list(CSD = "1102075"), level = "DA",
    geo_format = "sf") %>%
  st_transform(32620) %>%
  select(GeoUID, Dwellings) %>%
  set_names(c("GeoUID", "dwellings", "geometry")) %>%
  slice(1:3)

points <-
  points %>%
  st_intersection(polys) %>%
  select(property_ID) %>%
  rbind(points[751,1]) %>%
  mutate(property_ID = 1:31)
