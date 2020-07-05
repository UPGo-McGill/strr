### Code used to generate sample dataset for testing strr_process_property #####

library(tidyverse)

property_raw <-
  read_csv("Canada-montreal_Property_Match_2019-05-28.csv") %>%
  slice(1:50) %>%
  mutate(`Property ID` = paste0("ab-00", 101:150))

save(property_raw, file = "tests/testthat/strr_process_property_data.Rdata")
