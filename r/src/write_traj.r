#' write_traj writes out the output assosciated with each simulation step
#' @author James Mineau
#'
#' @param traj output created during simulation_step
#' @param file filename argument. Must end with .rds (for serialized R data
#'   output, preferred), .h5 (for Hierarchical Data Format output),
#'   or NULL to return the footprint object for programatic use.
#'   rds files do not require any additional libraries and have
#'   better compression. The \code{rhdf5} package is required for .h5 output.
#'   HDF5 output is slightly less efficient than .rds output, but is more
#'   portable and can be read by other languages.
#'
#' @import rhdf5
#' @export

write_traj <- function(traj, file) {

  # Remove existing trajectory file
  if (!is.null(file) && file.exists(file))
    system(paste('rm', file))

  # .rds output (preferred)
  if (!is.null(file) && grepl('\\.rds$', file, ignore.case = T)) {
    saveRDS(traj, file)
    return(file)
  }

  # .h5 output
  if (!is.null(file) && grepl('\\.h5$', file, ignore.case = T)) {
    # Check if rhdf5 package is installed
    if (!requireNamespace("rhdf5", quietly = TRUE)) {
      stop("The 'rhdf5' package is required for hdf5 output. Please install it.")
    }

    # Create HDF5 file
    fid <- rhdf5::H5Fcreate(file)

    # Write file info
    rhdf5::h5writeAttribute(file, fid, 'file')

    # Create groups
    rhdf5::h5createGroup(fid, 'receptor')
    rhdf5::h5createGroup(fid, 'params')

    # Write receptor data
    receptor <- rhdf5::H5Gopen(fid, 'receptor')
    rhdf5::h5writeAttribute(as.numeric(traj$receptor$run_time),
                            receptor, 'run_time')
    rhdf5::h5writeAttribute(traj$receptor$lati, receptor, 'lati')
    rhdf5::h5writeAttribute(traj$receptor$long, receptor, 'long')
    rhdf5::h5writeAttribute(traj$receptor$zagl, receptor, 'zagl')
    rhdf5::H5Gclose(receptor)

    # Write particle dataset
    rhdf5::h5write(traj$particle, fid, 'particle')

    # Create params group
    params <- rhdf5::H5Gopen(fid, 'params')
    for (p in names(traj$params)) {
      val <- traj$params[[p]]
      if (is.na(val)) val <- 'NA'
      rhdf5::h5writeAttribute(val, params, p)
    }
    rhdf5::H5Gclose(params)

    # Create particle error group if it exists
    if ('particle_error' %in% names(traj)) {

      # Write particle_error dataset
      rhdf5::h5write(traj$particle_error, fid, 'particle_error')

      # Create particle_error_param group
      rhdf5::h5createGroup(fid, 'particle_error_params')
      particle_error_params <- rhdf5::H5Gopen(fid, 'particle_error_params')
      for (p in names(traj$particle_error_params)) {
        val <- traj$particle_error_params[[p]]
        if (is.na(val)) val <- 'NA'
        rhdf5::h5writeAttribute(val, particle_error_params, p)
      }
      rhdf5::H5Gclose(particle_error_params)
    }

    # Close file
    rhdf5::H5Fclose(fid)

    return(file)
  }

  # .nc output
  if (!is.null(file) && grepl('\\.nc$', file, ignore.case = T)) {

    # Define dimensions
    #   receptor dims
    run_time <- ncdim_def('run_time', 'seconds since 1970-01-01 00:00:00Z',
                          as.numeric(traj$receptor$run_time),
                          longname = 'Receptor run time')
    lati <- ncdim_def('lati', 'degrees_north', traj$receptor$lati,
                      longname = 'Receptor latitude')
    long <- ncdim_def('long', 'degrees_east', traj$receptor$long,
                      longname = 'Receptor longitude')
    zagl <- ncdim_def('zagl', 'meters', traj$receptor$zagl,
                      longname = 'Receptor altitude above ground level')

    #   particle dims
    time <- ncdim_def('time', hycs_vars$time$units, 
                      sort(unique(traj$particle$time)),
                      longname = hycs_vars$time$longname)
    indx <- ncdim_def('indx', '', sort(unique(traj$particle$indx)),
                      longname = hycs_vars$indx$longname)

    #   param dim
    params <- 

    # Define variables
    var_names <- names(traj$particle)
    var_names <- vars[!(vars %in% c("indx", "time"))]

    vars <- list()
    var_data <- list()
    for (var in var_names) {
      ncvar <- ncvar_def(var, hycs_vars[[var]]$units, list(time, indx),
                         longname = hycs_vars[[var]]$longname)
      vars[[var]] <- ncvar

      # Pivot each column of particle data into a matrix with time as rows and indx as columns
      data <- tidyr::pivot_wider(traj$particle, names_from = indx, id_cols = time,
                                 values_from = var)
      data <- data[order(data$time), ] %>%
        select(-time)
      var_data[[var]] <- data
    }

    # Create netCDF file
    nc <- nc_create(file, vars, force_v4 = T)

    # Put variable data into netCDF file
    for (var in var_names) {
      ncvar_put(nc, vars[[var]], var_data[[var]])
    }

    nc_close(nc)
    return(file)
  }

  return(traj)
}

# variables output by hycs_std
hycs_vars = list(
  crai = list(
    longname = 'convective rainfall rate',
    units = 'm min-1'
  ),
  dens = list(
    longname = 'air density',
    units = 'kg m-3'
  ),
  dmas = list(
    longname = 'particle weight changes due to mass violation in wind fields [initial value = 1.0]',
    units = ''
  ),
  dswf = list(
    longname = 'downward shortwave radiation',
    units = 'W m-2'
  ),
  foot = list(
    longname = 'footprint, or sensitivity of mixing ratio to surface fluxes',
    units = 'ppm umol-1 m-2 s-1'
  ),
  icdx = list(
    longname = 'cloud index when using RAMS (Grell scheme) [1=updraft,2=environment,3=downdraft]',
    units = ''
  ),
  indx = list(
    longname = 'unique particle identifier',
    units = ''
  ),
  lati = list(
    longname = 'latitude position of particle',
    units = 'degrees_north'
  ),
  lcld = list(
    longname = 'low cloud cover',
    units = '%'
  ),
  lhtf = list(
    longname = 'latent heat flux',
    units = 'W m-2'
  ),
  long = list(
    longname = 'longitude position of particle',
    units = 'degrees_east'
  ),
  mlht = list(
    longname = 'mixed-layer height',
    units = 'm'
  ),
  rain = list(
    longname = 'total rainfall rate',
    units = 'm min-1'
  ),
  rhfr = list(
    longname = 'relative humidity fraction [0~1.0]',
    units = ''
  ),
  samt = list(
    longname = 'amount of time particle spends below VEGHT',
    units = 'hour'
  ),
  shtf = list(
    longname = 'sensible heat flux',
    units = 'W m-2'
  ),
  sigw = list(
    longname = 'standard deviation of vertical velocity; measure of strength of vertical turbulence',
    units = 'm s-1'
  ),
  solw = list(
    longname = 'soil moisture',
    units = ''
  ),
  sphu = list(
    longname = 'specific humidity',
    units = 'g g-1'
  ),
  tcld = list(
    longname = 'total cloud cover',
    units = '%'
  ),
  temp = list(
    longname = 'air temperature at lowest model layer',
    units = 'K'
  ),
  time = list(
    longname = 'time since start of simulation; negative if going backward in time',
    units = 'hour'
  ),
  tlgr = list(
    longname = 'Lagrangian decorrelation timescale',
    units = 's'
  ),
  wout = list(
    longname = 'vertical mean wind',
    units = 'm s-1'
  ),
  zagl = list(
    longname = 'vertical position of particle',
    units = 'm above ground level'
  ),
  zloc = list(
    longname = 'limit of convection heights',
    units = 'm'
  ),
  zsfc = list(
    longname = 'terrain height',
    units = 'm above sea level'
  )
)