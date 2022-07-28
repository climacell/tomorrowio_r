
## One day in seconds
days <- 60 * 60 * 24

## Oldest number of days that timeline API will support
timeline_start_limit <- 7

## Timeline API base URL
timeline_api_url <- "https://api.tomorrow.io/v4/timelines"

## Historical API base URL
historical_api_url <- "https://api.tomorrow.io/v4/historical"


#' Call Tomorrow.io weather API
#'
#' Fetch historical, current, and forecast weather data. Make calls to both Timeline and Historical API depending on the
#' range of dates requested.
#'
#' @param api_key `string` API key use to access [Tomorrow.io](https://www.tomorrow.io/weather-api/) services.
#' @param location 2 element `vector` of latitude and longitude location
#' @param fields  `list` of `string`s of [field names](https://docs.tomorrow.io/reference/data-layers-overview)
#' @param timesteps `string` [time step](https://docs.tomorrow.io/reference/data-layers-overview#timestep-availability)
#' @param start_time,end_time `POSIXct` start and end date and time inclusive to get data for.
#' @param units `string` unit name, possible values are `metric` or `imperial` (default is `metric`)
#' @param timezone `string` timezone in ISO 8601 format (default is `"UTC"`).
#' @param batch_size_days `numeric` batch size in days. When fetching a large data set, it is often
#'   better to make several API calls and combine the results. Passing in a value other than `0`
#'   will split into fixed sized batchs of the specified size in days. `0` will create a single
#'   batch (default is `0` which will make a single call to Timeline API, if needed, and multiple 29
#'   day calls to Historical, if needed).
#' @returns A single `data.frame` sorted by date with all data.
#' @examples
#' tomorrowio_api(
#'   api_key = API_KEY, location = c(42.3478, -71.0466), fields = c("temperature"), timesteps = c("1d"),
#'   start_time = Sys.time() - (60 * 60 * 24 * 30), end_time = Sys.time() + (60 * 60 * 24 * 10)
#' )
#' @export
tomorrowio_api <- function(api_key, location, fields, timesteps, start_time, end_time, units = "metric",
                           timezone = "UTC", batch_size_days = 0) {

  if (start_time > end_time) {
    stop("start_time has to be before end_time")
  }
  ## Initialize timeline and historical start and end times
  tl_start_time <- tl_end_time <- hi_start_time <- hi_end_time <- list()
  ## Timeline supports up to 7 days in the past
  if (Sys.time() - end_time > 7) {
    ## if we asked for an end_time more than 7 days in the past, call historical. Note that start_time
    ## has to also be in the past since start_time comes before.
    hi_end_time <- end_time
    hi_start_time <- start_time
  } else if (Sys.time() - start_time > timeline_start_limit) {
    ## If only start_time is more than 7 days in the past, split the call with historical.
    tl_start_time <- Sys.time() - (timeline_start_limit * days) # 7 days in the past
    tl_end_time <- end_time
    hi_start_time <- start_time
    hi_end_time <- tl_start_time
  } else {
    ## If both start_time and end_time are older than 7 days in the past, get everything from timeline
    tl_start_time <- start_time
    tl_end_time <- end_time
  }

  ## Initialize timeline and historical data.frames
  tl_df <- hi_df <- NULL
  ## If historical start time is initialized, we need to get data from timeline
  if (length(hi_start_time) > 0) {
    hi_df <- tomorrowio_historical(
      api_key = api_key, location = location, fields = fields, timesteps = timesteps,
      start_time = hi_start_time, end_time = hi_end_time, units = units, timezone = timezone,
      ## if batch size is zero, use the default of 29 days for historical, since the max is 30
      batch_size_days = if (batch_size_days == 0) 29 else batch_size_days
    )
  }
  ## If timeline start time is initialized, we need to get data from timeline
  if (length(tl_start_time) > 0) {
    tl_df <- tomorrowio_timeline(
      api_key = api_key, location = location, fields = fields, timesteps = timesteps,
      start_time = tl_start_time, end_time = tl_end_time, units = units, timezone = timezone,
      batch_size_days = batch_size_days
    )
  }
  ## Concat the to data frames into one
  df <- rbind(hi_df, tl_df)
  ## Remove duplicates based on the startTime
  df <- df[!duplicated(df$startTime), ]
  return(df)
}

#' Call Tomorrow.io Historical API
#'
#' Fetch historical wather data.
#'
#' @inheritParams tomorrowio_timeline
#' @param batch_size_days `numeric` batch size in days. When fetching a large data set, it is often
#'   better to make several API calls and combine the results. Passing in a value other than `0`
#'   will split into fixed sized batchs of the specified size in days. `0` will create a single
#'   batch (default is `29` since historical allows up to 30 days in a single query).
#' @returns A `data.frame`.
#' @examples
#' tomorrowio_historical(
#'   api_key = API_KEY, location = c(42.3478, -71.0466), fields = c("temperature"),
#'   timesteps = c("1d"), start_time = Sys.time() - (60 * 60 * 24 * 30),
#'   end_time = Sys.time() - (60 * 60 * 24 * 20)
#'  )
#' @export
tomorrowio_historical <- function(api_key, location, fields, timesteps, start_time, end_time, units = "metric",
                                   timezone = "UTC", batch_size_days = 29) {
  return(batch_api_helper(
    "historical", api_key = api_key, location = location, fields = fields, timesteps = timesteps,
      start_time = start_time, end_time = end_time, units = units, timezone = timezone,
      batch_size_days = batch_size_days
  ))

}

#' Call Tomorrow.io Timeline API
#'
#' Fetch current and forecast weather data.
#'
#' @inheritParams tomorrowio_api
#' @returns A `data.frame`.
#' @examples
#' tomorrowio_timeline(
#'   api_key = API_KEY, location = c(42.3478, -71.0466), fields = c("temperature"), timesteps = c("1d"),
#'   start_time = Sys.time() - (60 * 60 * 24 * 3), end_time = Sys.time() + (60 * 60 * 24 * 5)
#' )
#' @export

tomorrowio_timeline <- function(api_key, location, fields, timesteps, start_time, end_time, units = "metric",
                                timezone = "UTC", batch_size_days = 0) {
  return(batch_api_helper(
    "timeline", api_key = api_key, location = location, fields = fields, timesteps = timesteps,
      start_time = start_time, end_time = end_time, units = units, timezone = timezone,
      batch_size_days = batch_size_days
  ))
}

batch_api_helper <- function(endpoint, api_key, location, fields, timesteps, start_time, end_time, units = "metric",
                             timezone = "UTC", batch_size_days = 0) {
  if (start_time > end_time) {
    ## start_time has to be before end_time
    stop("start_time has to be before end_time")
  }

  ## Initialize batch window start time and end time
  batch_start_time <- start_time
  ## End time should be the smallest of end tiem or the first batch, if batch size is set.
  batch_end_time <- if (batch_size_days <= 0) end_time else min(end_time, batch_start_time + (batch_size_days * days))
  ## Initialize global data frame
  gdf <- NULL
  repeat {
    ## Get the data from the API
    df <- api_helper(endpoint, api_key, location, fields, units, timesteps, batch_start_time, batch_end_time, timezone)
    ## Append new df to global data frame
    gdf <- if (is.null(gdf)) df else rbind(gdf, df)
    ## Step forward to the next batch window
    batch_start_time <- batch_end_time
    batch_end_time <- min(end_time, batch_end_time + (batch_size_days * days))
    ## If we are beyond the overall end time, we are done
    if (batch_start_time >= end_time) {
      break
    }
  }
  return(gdf)
}

api_helper <- function(endpoint, api_key, location, fields, units, timesteps, start_time, end_time, timezone) {
  if (start_time > end_time) {
    stop("start_time has to be before end_time")
  }
  ## Find the right URL to use based on the endpoint
  if (endpoint == "timeline") {
    url <- timeline_api_url
  } else if (endpoint == "historical") {
    url <- historical_api_url
  } else {
    stop(sprintf("'%s' is not a value end point (possible values: timeline, historical)", endpoint))
  }
  ## Create the POST payload
  payload <- paste(
    sprintf('{"location":"%s"', paste(location, collapse = ",")),
    sprintf('"fields":["%s"]', paste(fields, collapse = "\",\"")),
    sprintf('"units":"%s"', units),
    sprintf('"timesteps":["%s"]', paste(timesteps, collapse = "\",\"")),
    ## Use UTC to ensure that the API returns the right data.
    sprintf('"startTime":"%s"', format(start_time, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")),
    sprintf('"endTime":"%s"', format(end_time, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")),
    sprintf('"timezone":"%s"}', timezone),
    sep = ","
  )
  ## Call the API
  response <- httr::POST(url,
    body = payload, httr::add_headers("Accept-Encoding" = "gzip"), query = list(apikey = api_key),
    httr::content_type("application/json"), httr::accept("application/json"), encode = "json"
    )
  ## Turn resonse content into a string
  cont <- httr::content(response, "text")
  ## Check the status code
  if (response$status_code >= 300) {
    stop(sprintf("Error code %i while querying %s data: %s", response$status_code, endpoint, cont))
  }
  ## Parse the json
  document <- jsonlite::fromJSON(cont, simplifyVector = TRUE)
  ## Get the timelines data frame, each row is one timestep
  tl <- document$data$timelines
  gdf <- NULL
  ## Loop over each timestep
  for (i in  seq_len(nrow(tl))) {
    ## Grab one row at a time.
    t <- tl[i, ]
    df <- data.frame(t$intervals)
    ## Replace string with a real date/time object
    df$startTime <- as.POSIXct(df$startTime, "%Y-%m-%dT%H:%M:%S", tz = timezone)
    ## Create some factors
    df$timestep  <- factor(t$timestep)
    df$endpoint  <- factor(endpoint)
    ## Values is a sub data frame stored in one column. So we flatten it by adding it as new columns.
    df <- cbind(df, df$values)
    ## Then drop the values sub data frame
    df <- df[, !(names(df) %in% c("values"))]
    ## Append new df to global data frame
    gdf <- if (is.null(gdf)) df else rbind(gdf, df)
  }
  return(gdf)
}
