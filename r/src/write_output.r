#' write_output writes out the output assosciated with each simulation step
#' @author James Mineau
#'
#' @param rundir The directory to write the output to
#' @param simulation_id The unique identifier for the simulation
#' @param output A list containing the following keys:
#'   - receptor: The receptor information: run_time, lati, long, zagl
#'   - namelist: The HYSPLIT namelist
#'   - params: The HYSPLIT parameters
#'   - met_files: The met files used in the simulation
#'   - particle: The particle trajectory data
#'   - particle_error: The error data if it exists (optional)
#'   - particle_error_params: The HYSPLIT error parameters if they exist (optional)
#' @param write_trajec Logical indicating whether to write the trajectory output to disk. Defaults to TRUE.
#' @return The path to the trajectory file
#' 
#' @import arrow
#' @import jsonlite
#' @export

write_output <- function(rundir, simulation_id, output,
                         write_trajec = T) {

  # Define output files
  input_file <- file.path(rundir, paste0(simulation_id, '_input.json'))
  traj_file <- file.path(rundir, paste0(simulation_id, '_traj.parquet'))
  error_file <- file.path(rundir, paste0(simulation_id, '_error.parquet'))

  # Remove existing output files if they exist
  for (file in c(input_file, traj_file, error_file)) {
    if (file.exists(file))
      system(paste('rm', file))
  }

  # Merge namelist, params, and receptor
  input <- list(receptor=output$receptor,
                namelist=output$namelist,
                params=output$params,
                met_files=output$met_files)
  
  # Add error data if it exists
  if (!is.null(output$particle_error) & write_trajec) {
    # Write error to parquet
    write_parquet(output$particle_error, error_file)

    # Add error parameters to input
    input$particle_error_params <- output$particle_error_params
  }

  # Write input to JSON
  write_json(input, input_file, pretty=T, auto_unbox=T)

  if (write_trajec) {
    # Write trajectory to parquet
    write_parquet(output$particle, traj_file)
  } else {
    traj_file <- NULL
  }

  return(traj_file)
}
