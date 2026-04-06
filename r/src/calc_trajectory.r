#' calc_footprint generates upstream influence footprint
#' @author Ben Fasoli
#'
#' Aggregates the upstream particle trajectories into a time integrated
#' footprint, expanding particle influence using variable 2d gaussian kernels
#' with bandwidths proportional to the mean pairwise distance between all
#' particles at each time step. Requires compiled permute.so to build the
#' gaussian kernels with fortran.
#'
#' For documentation, see https://uataq.github.io/stilt/
#'
#' @import arrow
#' @export

calc_trajectory <- function(namelist,
                            simulation_dir,
                            emisshrs,
                            hnf_plume,
                            met_files,
                            n_hours,
                            receptor,
                            rm_dat,
                            timeout,
                            w_option,
                            z_top,
                            file = NULL) {

  # Enable manual rescaling of mixed layer height
  if (as.logical(namelist[['zicontroltf']])) {
    write_zicontrol(namelist[['ziscale']], file.path(simulation_dir, 'ZICONTROL'))
  }

  # Write SETUP.CFG and CONTROL files to control model
  do.call(write_setup, 
          merge_lists(namelist, list(file = file.path(simulation_dir, 'SETUP.CFG'))))
  write_control(receptor, emisshrs, n_hours, w_option, z_top, met_files,
                file.path(simulation_dir, 'CONTROL'))

  # Simulation timeout ---------------------------------------------------------
  # Monitors time elapsed running hycs_std If elapsed time exceeds timeout
  # specified in run_stilt.r, kills hycs_std and moves on to next simulation
  cmd <- paste0('cd ', simulation_dir, ' && ./hycs_std >> stilt.log 2>&1')
  system(cmd, timeout = timeout)

  # Exit if running in HYSPLIT mode
  if (namelist[['ichem']] != 8) return()

  pf <- file.path(simulation_dir, 'PARTICLE_STILT.DAT')
  if (!file.exists(pf)) {
    msg <- paste('Failed to output ', pf)
    warning(msg)
    cat(msg, '\n', file = file.path(simulation_dir, 'stilt.log'), append = T)
    return()
  }

  n_lines <- count_lines(pf)
  if (n_lines < 2) {
    msg <- paste(pf, 'does not contain any trajectory data.')
    warning(msg)
    cat(msg, '\n', file = file.path(simulation_dir, 'stilt.log'), append = T)
    return()
  }

  # Read particle file, optionally remove PARTICLE.DAT in favor of compressed
  # .rds file, and return particle data frame
  p <- read_particle(file = pf, varsiwant = namelist[['varsiwant']])
  if (rm_dat) {
    system(paste('rm', pf))
    system(paste('rm', file.path(simulation_dir, 'PARTICLE.DAT')))
  }

  numpar <- max(p$indx)

  # For Column & MultiPoint trajectories, preserve release height as xhgt
  if (receptor$kind == "Column") {
    # Particles are distributed vertically in a layer between the bottom and top
    xhgt_min <- min(receptor$locations$zagl)
    xhgt_max <- max(receptor$locations$zagl)
    xhgt_rng <- xhgt_max - xhgt_min
    xhgt_step <- xhgt_rng / numpar

    px <- data.frame(indx = 1:numpar)    
    px$xhgt <- (px$indx - 0.5) * xhgt_step + xhgt_min
    p <- merge(p, px, by = 'indx', sort = F)
  } else if (receptor$kind == "MultiPoint") {
    # Particles are evenly distributed between each point
    # Particles are batch assigned to a location based on the order of receptors in the CONTROL file
    n_locs <- nrow(receptor$locations)
    particles_per_loc <- rep(floor(numpar / n_locs), n_locs)
    remainder <- numpar %% n_locs
    if (remainder > 0) {
      particles_per_loc[1:remainder] <- particles_per_loc[1:remainder] + 1
    }

    xhgt <- rep(receptor$locations$zagl, times = particles_per_loc)
    px <- data.frame(indx = 1:numpar, xhgt = xhgt)
    p <- merge(p, px, by = 'indx', sort = FALSE)
  } else if (receptor$kind != "Point") {
    stop("calc_trajectory(): Receptor kind (", receptor$kind, ") is not fully supported.")
  }

  # Calculate near-field dilution height based on gaussian plume width
  # approximation and recalculate footprint sensitivity for cases when the
  # plume height is less than the PBL height scaled by veght
  if (hnf_plume) {
    p <- calc_plume_dilution(p, numpar, receptor$locations$zagl, namelist[['veght']])
  }

  if (!is.null(file))  {
    # Add absolute datetime column (p$time is minutes from receptor time)
    p$datetime <- receptor$time + p$time * 60
    # Write particle data to parquet file
    write_parquet(p, file)
  }

  return(p)
}
