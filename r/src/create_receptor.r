#' Create a receptor object for STILT simulations
#' @author James Mineau
#'
#' Create a receptor object containing time and locations for STILT simulations.
#' Determines the kind of receptor based on the number of unique locations
#' and heights.
#'
#' @param time POSIXct object representing the time of the receptor
#' @param lati numeric vector or list of latitudes in degrees
#' @param long numeric vector or list of longitudes in degrees
#' @param zagl numeric vector or list of heights above ground level in meters
#' @return A list containing the receptor time, kind, and locations.
#'  The kind can be "Point", "Column", "MultiColumn", "Mixed", or "MultiPoint".
#'  The locations data frame contains the latitude, longitude, and height above
#'  ground level for each receptor location.
#'
#' @import dplyr
#' @export

create_receptor <- function(time, lati, long, zagl) {
  require(dplyr)

  if (length(time) != 1 || !inherits(time, "POSIXct")) {
    stop("create_receptor(): time must be a single POSIXct object")
  }

  # Create the core locations data frame
  locations <- data.frame(
    lati = unlist(lati),
    long = unlist(long),
    zagl = unlist(zagl),
    stringsAsFactors = FALSE
  ) %>%
    # Keep only min and max zagl for each unique lati/long pair
    group_by(lati, long) %>%
    mutate(
      is_min = zagl == min(zagl),
      is_max = zagl == max(zagl)
    ) %>%
    filter(
      is_min | is_max
    ) %>%
    ungroup() %>%
    select(-is_min, -is_max)

  # Determine kind
  loc_summary <- locations %>%
    group_by(lati, long) %>%
    summarise(n_heights = n_distinct(zagl), .groups = 'drop')
  n_unique_groups <- nrow(loc_summary)
  n_columns <- sum(loc_summary$n_heights > 1)
  n_points <- sum(loc_summary$n_heights == 1)

  if (n_unique_groups == 1 && loc_summary$n_heights[1] == 1) {
    kind <- "Point"
  } else if (n_unique_groups == 1 && loc_summary$n_heights[1] > 1) {
    kind <- "Column"
  } else if (n_columns > 1 && n_points == 0) {
    kind <- "MultiColumn"
  } else if (n_columns > 0 && n_points > 0) {
    kind <- "Mixed"
  } else if (n_unique_groups > 1 && all(loc_summary$n_heights == 1)) {
    kind <- "MultiPoint"
  } else {
    kind <- "Unknown"
  }

  if (kind %in% c("MultiColumn", "Mixed", "Unknown")) {
    stop("create_receptor(): Receptor kind (", kind, ") is not fully supported.")
    # TODO - need to determine how to calculate xhgt for these kinds
  }

  # Assemble the final receptor object
  receptor <- list(
    time = time,
    kind = kind,
    locations = locations
  )

  return(receptor)
}
