### Code used to generate sample dataset for testing strr_process_review #######

library(tidyverse)

review_raw <-
  read_csv("All_1Month_Review_2019-06-26.csv")

review_raw <- review_raw[1:1000,]

IDs <- unique(review_raw$`User ID`)

load("latest_user.Rdata")

latest_user_all <- filter(latest_user, user_ID %in% IDs)

latest_user <- filter(latest_user_all, date < "2019-05-01")

save(review_raw, latest_user, latest_user_all,
     file = "tests/testthat/strr_process_review_data.Rdata")
