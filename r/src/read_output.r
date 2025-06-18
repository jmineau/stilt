#' read_output reads in the output assosciated with each simulation step
#' @author James Mineau
#'
#' @param rundir The directory to read the output from. This directory should
#'   contain the simulation ID as its name and should have the following files:
#'   - `<simulation_id>_config.json`: The configuration file containing receptor,
#'     namelist, params, and met_files.
#'   - `<simulation_id>_traj.parquet`: The trajectory data in Parquet format.
#'   - `<simulation_id>_error.parquet`: The error data in Parquet format (optional).
#' @return A list containing the following keys:
#'   - receptor: The receptor information: run_time, lati, long, zagl
#'   - namelist: The HYSPLIT namelist
#'   - params: The HYSPLIT parameters
#'   - met_files: The met files used in the simulation
#'   - particle: The particle trajectory data
#'   - particle_error: The error data if it exists (optional)
#'   - particle_error_params: The HYSPLIT error parameters if they exist (optional)
#' 
#' @import arrow
#' @import jsonlite
#' @export

read_output <- function(rundir) {

  simulation_id <- basename(rundir)

  # Define output files
  config_file <- file.path(rundir, paste0(simulation_id, '_config.json'))
  traj_file <- file.path(rundir, paste0(simulation_id, '_traj.parquet'))
  error_file <- file.path(rundir, paste0(simulation_id, '_error.parquet'))

  # If trajectory file does not exist, return NULL
  if (!file.exists(traj_file)) {
    return(NULL)
  }

  # Read config JSON file
  config <- read_json(config_file)

  # Read trajectory parquet file
  particle <- read_parquet(traj_file)

  # Initialize output list
  output <- list(
    receptor = config$receptor,
    namelist = config$namelist,
    params = config$params,
    particle = particle,
    met_files = config$met_files
  )

  # Read error data if it exists
  if (file.exists(error_file)) {
    output$particle_error <- read_parquet(error_file)
    output$particle_error_params <- config$particle_error_params
  }

  return(output)
}