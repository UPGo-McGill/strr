### Code used to generate sample dataset for testing strr_process_property #####

library(tidyverse)

daily_raw <- read_csv("Canada-montreal_Daily_Match_2019-05-28.csv")

daily_raw_list <- group_split(daily_raw, `Property ID`)

daily_raw_list <- daily_raw_list[1:50]

daily_raw <- map2_dfr(daily_raw_list, 101:150, ~{
  .x %>% mutate(`Property ID` = paste0("ab-00", .y))
})

save(daily_raw, file = "tests/testthat/strr_process_daily_data.Rdata")

strr_process_daily(daily_raw, strr_process_property(property_raw)[[1]])
