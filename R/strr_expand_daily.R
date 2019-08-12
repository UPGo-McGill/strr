strr_expand_daily <- function(daily, start = NULL, end = NULL, cores = 1) {

  ## ERROR CHECKING AND ARGUMENT INITIALIZATION

  # Check that cores is an integer > 0

  cores <- floor(cores)
  if (cores <= 0) {
    stop("The argument `cores` must be a positive integer.")
  }

  # Check that dates are coercible to date class, then coerce them

  if (!is.null(start)) {
    start <- tryCatch(as.Date(start), error = function(e) {
      stop(paste0('The value of `start`` ("', start,
                  '") is not coercible to a date.'))
    })}

  if (!is.null(end)) {
    end <- tryCatch(as.Date(end), error = function(e) {
      stop(paste0('The value of `end` ("', end,
                  '") is not coercible to a date.'))
    })}


  ## PREPARE DATE FIELD

  daily <-
    daily %>%
    mutate(date = map2(start_date, end_date, ~{.x:.y}))


  ## SINGLE-CORE VERSION

  if (cores == 1) {

    daily <-
      daily %>%
      unnest() %>%
      mutate(date = as.Date(date, origin = "1970-01-01")) %>%
      select(property_ID, date, status:res_id)

  ## MULTI-CORE VERSION

  } else {

    daily_list <-
      split(daily, 0:(nrow(daily) - 1) %/% ceiling(nrow(daily) / 100))

    daily <-
      pblapply(daily_list, function(x) {
        x %>%
          unnest() %>%
          mutate(date = as.Date(date, origin = "1970-01-01")) %>%
          select(property_ID, date, status:res_id)
      }) %>%
      bind_rows()
  }


  ## OPTIONALLY TRIM BASED ON START/END DATE

  if (!is.null(start)) {
    daily <- filter(daily, date >= start)
  }

  if (!is.null(end)) {
    daily <- filter(daily, date <= end)
  }


  ## OUTPUT DATA FRAME

  return(daily)
}
