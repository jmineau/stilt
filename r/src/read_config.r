#' Read a configuration file and return a list of parameters.
#' @author James Mineau
#'
#' Reads a YAML configuration file for STILT simulations and returns a flat list of parameters.
#' 
#' @param file Path to the YAML configuration file.
#' @return A list containing the configuration parameters.
#'
#' @import yaml
#' @export

read_config <- function(file) {
  require(yaml)

  # Read the YAML file
  config <- read_yaml(file)

  # Unnest the first level of lists in config
  for (k in c("model", "footprint", "met", "transport", "error", "user_funcs")) {
    if (!is.null(config[[k]])) {
      config <- merge_lists(config, config[[k]])
      config[[k]] <- NULL
    }
  }

  # Remove null values from config to use default simulation_step arguments
  config <- config[!sapply(config, is.null)]

  return(config)
}