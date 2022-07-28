# tomorrowio_r

Tomorrow.io Weather Data API R package

# Installation

    library("remotes")
    install_github("climacell/tomorrowio_r")

# Example

    library("tomorrowior")

    api_key = "REPLACE WITH YOUR API KEY"
    start_time <- Sys.time() - (60 * 60 * 24 * 30)
    end_time <- Sys.time() + (60 * 60 * 24 * 10)
    location <- c(42.3478, -71.0466)
    fields <- c("temperature", "windSpeed")
    timesteps <- c("1h", "1d")

    df <- tomorrowio_api(api_key = api_key, location = location, fields = fields, timesteps = timesteps, start_time = start_time, end_time = end_time)

# License

[MIT](https://opensource.org/licenses/MIT)
