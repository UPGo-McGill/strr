strr_expand_daily <- function(daily, start = NULL, end = NULL) {

  # Expand each date range to one-row-per-date
  daily <-
    setDT(daily)[, list(apid = apid,
                        date = seq(start_date, end_date, by = "day"),
                        status = status, rid = rid, cost = cost),
                 by = 1:nrow(daily)]

  # Optionally trim based on start date
  if (!is.null(start)) {
    start <- as.Date(start)
    daily <- daily[date >= start]
  }

  # Optionally trim based on end date
  if (!is.null(end)) {
    end <- as.Date(end)
    daily <- daily[date <= end]
  }

  # Output tibble
  daily %>%
    as_tibble() %>%
    select(-nrow)
}
