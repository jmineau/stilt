#' simulation_step runs STILT for the given receptor
#' @author Ben Fasoli, updated by James Mineau
#'
#' Executes trajectory calculations with (and optionally without) transport
#' error and calculates kernel density derived footprint grids.
#'
#' For documentation, see https://jmineau.github.io/stilt/
#'
#' @export

simulation_step <- function(
  # System configuration
  stilt_wd = getwd(),
  output_wd = file.path(stilt_wd, 'out'),
  lib.loc = NULL,
  # Receptor placement
  r_time,
  r_lati,
  r_long,
  r_zagl,
  # Footprint calculation methods
  hnf_plume = T,
  projection = '+proj=longlat',
  smooth_factor = 1,
  time_integrate = F,
  xmn = NA,
  xmx = NA,
  xres = NA,
  ymn = NA,
  ymx = NA,
  yres = xres,
  foot_id = '',
  # Meteorological data input
  met_path,
  met_file_format,
  met_file_tres = '6 hours',
  met_subgrid_buffer = 0.1,
  met_subgrid_enable = F,
  met_subgrid_levels = NA,
  n_met_min = 1,
  # Model control
  n_hours = -24,
  numpar = 1000,
  rm_dat = T,
  run_foot = T,
  run_trajec = T,
  simulation_id = NA,
  timeout = 3600,
  varsiwant = c('time', 'indx', 'long', 'lati',
                'zagl', 'foot', 'mlht', 'pres',
                'dens', 'samt', 'sigw', 'tlgr'),
  # Transport and dispersion
  capemin = -1,
  cmass = 0,
  conage = 48,
  cpack = 1,
  delt = 1,
  dxf = 1,
  dyf = 1,
  dzf = 0.01,
  efile = '',
  emisshrs = 0.01,
  frhmax = 3,
  frhs = 1,
  frme = 0.1,
  frmr = 0,
  frts = 0.1,
  frvs = 0.1,
  hscale = 10800,
  ichem = 8,
  idsp = 2,
  initd = 0,
  k10m = 1,
  kagl = 1,
  kbls = 1,
  kblt = 5,
  kdef = 0,
  khinp = 0,
  khmax = 9999,
  kmix0 = 250,
  kmixd = 3,
  kmsl = 0, 
  kpuff = 0,
  krand = 4,
  krnd = 6,
  kspl = 1,
  kwet = 1,
  kzmix = 0,
  maxdim = 1,
  maxpar = numpar,
  mgmin = 10,
  mhrs = 9999,
  nbptyp = 1,
  ncycl = 0,
  ndump = 0,
  ninit = 1,
  nstr = 0,
  nturb = 0,
  nver = 0,
  outdt = 0,
  p10f = 1,
  pinbc = '',
  pinpf = '',
  poutf = '',
  qcycle = 0,
  rhb = 80,
  rht = 60,
  splitf = 1,
  tkerd = 0.18,
  tkern = 0.18,
  tlfrac = 0.1,
  tout = 0,
  tratio = 0.75,
  tvmix = 1,
  veght = 0.5,
  vscale = 200,
  vscaleu = 200,
  vscales = -1,
  w_option = 0,
  wbbh = 0,
  wbwf = 0,
  wbwr = 0,
  wvert = FALSE,
  z_top = 25000,
  zicontroltf = 0,
  ziscale = 0,
  # Transport error calculations
  siguverr = NA,
  tluverr = NA,
  zcoruverr = NA,
  horcoruverr = NA,
  sigzierr = NA,
  tlzierr = NA,
  horcorzierr = NA,
  # User defined functions
  before_footprint = NA,
  # User defined arguments
  ...
  ) {
  try({
    setwd(stilt_wd)

    args <- list(...)  # accept additional arguments (currently unused)

    # Validate arguments
    if (!run_trajec && !run_foot)
      stop('simulation_step(): Nothing to do, set run_trajec or run_foot to T')

    if ((met_subgrid_enable || run_foot) && (is.na(xmn) || is.na(xmx) || is.na(xres) || is.na(ymn) || is.na(ymx))) {
      stop("simulation_step(): xmn, xmx, xres, ymn, and ymx must be specified when met_subgrid_enable or run_foot is TRUE")
    }

    # Get before_footprint function
    if (!is.na(before_footprint)) {
      before_footprint_file <- file.path(stilt_wd, 'r', 'user', 'before_footprint.r')
      if (is.list(before_footprint)) {
        before_footprint <- before_footprint[[1]]
      }
      if (is.function(before_footprint)) {
        # Write the function to a file if it is not already
        if (!file.exists(before_footprint_file)) {
          dump(before_footprint, file = before_footprint_file)
        }
      } else if (is.character(before_footprint)) {
        if (file.exists(before_footprint)) {
          before_footprint_file <- before_footprint
          source(before_footprint)
          before_footprint <- get('before_footprint')
        } else {
          stop("simulation_step(): before_footprint must be a function or a file path to a function")
        }
      }
    } else {
      # Set a default function that returns 'output'
      before_footprint_file <- NA
      before_footprint <- function(output) { output }
    }
    # Ensure user specified functions reference the simulation_step environment
    environment(before_footprint) <- environment()

    # Vector style arguments passed as a list
    varsiwant <- unlist(varsiwant)
    ziscale <- unlist(ziscale)

    # Ensure dependencies are loaded for current node/process
    source(file.path(stilt_wd, 'r/dependencies.r'), local = T)

    # Create receptor object
    receptor <- create_receptor(
      time = r_time,
      lati = r_lati,
      long = r_long,
      zagl = r_zagl
    )

    # Build the simulation id from receptor info
    if (is.na(simulation_id) || is.null(simulation_id) || simulation_id == '') {
      if (receptor$kind == "Point") {
        simulation_id_format <- paste0('%Y%m%d%H%M_',
                                       r_long, '_', r_lati, '_', r_zagl)
        simulation_id <- strftime(r_time, simulation_id_format, 'UTC')
      } else if (receptor$kind == "Column") {
        simulation_id_format <- paste0('%Y%m%d%H%M_',
                                       r_long[1], '_', r_lati[1], '_X')
        simulation_id <- strftime(r_time, simulation_id_format, 'UTC')
      } else if (receptor$kind == "MultiPoint") {
        # Generate a unique simulation ID based on md5 hash of receptor locations
        hash <- digest::digest(receptor$locations, algo = "md5")
        simulation_id_format <- paste0('%Y%m%d%H%M_multi_', hash)
        simulation_id <- strftime(r_time, simulation_id_format, 'UTC')
      } else {
        stop("simulation_step(): Unsupported receptor kind: ", receptor$kind)
      }
    }
    if (grepl(.Platform$file.sep, simulation_id, fixed = TRUE)) {
      stop("simulation_step(): simulation_id must not contain path separators")
    }

    # Creates subdirectories in out for each simulation. Each of these
    # subdirectories is populated with symbolic links to the shared datasets
    # below and a simulation-specific SETUP.CFG and CONTROL
    simulation_dir <- file.path(output_wd, 'by-id', simulation_id)
    dir.create(simulation_dir, showWarnings = F, recursive = T)
    dir.create(file.path(output_wd, 'particles'), showWarnings = F, recursive = T)
    dir.create(file.path(output_wd, 'footprints'), showWarnings = F, recursive = T)
    message(paste('Running simulation ID:  ', simulation_id))

    # Write a custom configuration yaml file for reproducibility
    config = list(
      # System configuration
      stilt_wd = stilt_wd,
      output_wd = output_wd,
      # Receptor placement
      r_lati = r_lati,
      r_long = r_long,
      r_time = r_time,
      r_zagl = r_zagl,
      # Footprint calculation methods
      hnf_plume = hnf_plume,
      projection = projection,
      smooth_factor = smooth_factor,
      time_integrate = time_integrate,
      xmn = xmn,
      xmx = xmx,
      xres = xres,
      ymn = ymn,
      ymx = ymx,
      yres = yres,
      foot_id = foot_id,
      # Meteorological data input
      met_path = met_path,
      met_file_format = met_file_format,
      met_file_tres = met_file_tres,
      met_subgrid_buffer = met_subgrid_buffer,
      met_subgrid_enable = met_subgrid_enable,
      met_subgrid_levels = met_subgrid_levels,
      n_met_min = n_met_min,
      # Model control
      n_hours = n_hours,
      numpar = numpar,
      rm_dat = rm_dat,
      run_foot = run_foot,
      run_trajec = run_trajec,
      simulation_id = simulation_id,
      timeout = timeout,
      varsiwant = varsiwant,
      # Transport and dispersion
      capemin = capemin,
      cmass = cmass,
      conage = conage,
      cpack = cpack,
      delt = delt,
      dxf = dxf,
      dyf = dyf,
      dzf = dzf,
      efile = efile,
      emisshrs = emisshrs,
      frhmax = frhmax,
      frhs = frhs,
      frme = frme,
      frmr = frmr,
      frts = frts,
      frvs = frvs,
      hscale = hscale,
      ichem = ichem,
      idsp = idsp,
      initd = initd,
      k10m = k10m,
      kagl = kagl,
      kbls = kbls,
      kblt = kblt,
      kdef = kdef,
      khinp = khinp,
      khmax = khmax,
      kmix0 = kmix0,
      kmixd = kmixd,
      kmsl = kmsl,
      kpuff = kpuff,
      krand = krand,
      krnd = krnd,
      kspl = kspl,
      kwet = kwet,
      kzmix = kzmix,
      maxdim = maxdim,
      maxpar = maxpar,
      mgmin = mgmin,
      mhrs = mhrs,
      nbptyp = nbptyp,
      ncycl = ncycl,
      ndump = ndump,
      ninit = ninit,
      nstr = nstr,
      nturb = nturb,
      nver = nver,
      outdt = outdt,
      p10f = p10f,
      pinbc = pinbc,
      pinpf = pinpf,
      poutf = poutf,
      qcycle = qcycle,
      rhb = rhb,
      rht = rht,
      splitf = splitf,
      tkerd = tkerd,
      tkern = tkern,
      tlfrac = tlfrac,
      tout = tout,
      tratio = tratio,
      tvmix = tvmix,
      veght = veght,
      vscale = vscale,
      vscaleu = vscaleu,
      vscales = vscales,
      w_option = w_option,
      wbbh = wbbh,
      wbwf = wbwf,
      wbwr = wbwr,
      wvert = wvert,
      z_top = z_top,
      zicontroltf = zicontroltf,
      ziscale = ziscale,
      # Transport error calculations
      siguverr = siguverr,
      tluverr = tluverr,
      zcoruverr = zcoruverr,
      horcoruverr = horcoruverr,
      sigzierr = sigzierr,
      tlzierr = tlzierr,
      horcorzierr = horcorzierr,
      winderrtf = 0,
      # User defined functions
      before_footprint = before_footprint_file
    )
    config_file <- file.path(simulation_dir, paste0(simulation_id, '_config.yaml'))
    do.call(write_config, merge_lists(config, list(file=config_file)))

    # Calculate particle trajectories ------------------------------------------
    # run_trajec determines whether to try using existing trajectory files or to
    # recycle existing files
    trajec_file <- file.path(simulation_dir, paste0(simulation_id, '_trajec.parquet'))
    error_file <- file.path(simulation_dir, paste0(simulation_id, '_error.parquet'))
    winderr_file <- file.path(simulation_dir, 'WINDERR')
    zierr_file <- file.path(simulation_dir, 'ZIERR')

    output <- list()

    if (run_trajec) {
      # Ensure necessary files and directory structure are established in the
      # current simulation_dir
      if (!dir.exists(simulation_dir)) dir.create(simulation_dir)

      exe <- file.path(stilt_wd, 'exe')
      link_files(exe, simulation_dir)

      # Find necessary met files
      met_files <- find_met_files(receptor$time, n_hours, met_path,
                                  met_file_format, met_file_tres)
      if (length(met_files) < n_met_min) {
        msg <- paste('Insufficient number of meteorological files found. Check',
                     'specifications in run_stilt.r')
        warning(msg)
        cat(msg, '\n', file = file.path(simulation_dir, 'stilt.log'), append = T)
        return()
      }

      if (met_subgrid_enable) {
        met_path <- file.path(output_wd, 'met')
        calc_met_subgrids(met_files, met_path, exe,
                          projection, xmn, xmx, ymn, ymx,
                          levels = met_subgrid_levels,
                          met_subgrid_buffer = met_subgrid_buffer)

        # Find necessary met files for subgrids
        met_files <- find_met_files(receptor$time, n_hours, met_path,
                                    met_file_format, met_file_tres)
        if (length(met_files) < n_met_min) {
          msg <- paste('Insufficient number of meteorological files found. Check',
                      'specifications in run_stilt.r')
          warning(msg)
          cat(msg, '\n', file = file.path(simulation_dir, 'stilt.log'), append = T)
          return()
        }
      }

      # Calculate trajectory
      particle <- calc_trajectory(
        simulation_dir = simulation_dir,
        namelist = get_namelist_from_config(config),
        emisshrs = emisshrs,
        hnf_plume = hnf_plume,
        met_files = met_files,
        n_hours = n_hours,
        receptor = receptor,
        rm_dat = rm_dat,
        timeout = timeout,
        w_option = w_option,
        z_top = z_top,
        file = trajec_file
      )
      if (is.null(particle)) return()

      # Bundle trajectory configuration metadata with trajectory informtation
      output$particle <- particle
      output$params <- read_cfg(file = file.path(simulation_dir, 'CONC.CFG'))

      # Optionally execute second trajectory simulations to quantify transport
      # error using parameterized correlation length and time scales
      xyerr <- write_winderr(siguverr, tluverr, zcoruverr, horcoruverr,
                             file = winderr_file)
      zerr <- write_zierr(sigzierr, tlzierr, horcorzierr,
                          file = zierr_file)
      winderrtf <- (!is.null(xyerr)) + 2 * !is.null(zerr)
      if (winderrtf > 0) {
        config$winderrtf <- winderrtf

        # Calculate transport error
        particle_error <- calc_trajectory(
          simulation_dir = simulation_dir,
          namelist = get_namelist_from_config(config),
          emisshrs = emisshrs,
          hnf_plume = hnf_plume,
          met_files = met_files,
          n_hours = n_hours,
          receptor = receptor,
          rm_dat = rm_dat,
          timeout = timeout,
          w_option = w_option,
          z_top = z_top,
          file = error_file
        )
        if (is.null(particle_error)) return()
        output$particle_error <- particle_error
      }

      # Symlink trajectory to out/particles
      link_files(trajec_file, file.path(output_wd, 'particles'))

    } else {
      # If user opted to recycle existing trajectory files, read in the recycled
      # file to a data frame. If none exists, report an error and proceed

      if (!file.exists(trajec_file)) {
        warning('simulation_step(): No trajectory file found in ', simulation_dir,
                '\n  skipping this receptor and trying the next...')
        return()
      }

      # Read the trajectory file and error file if it exists
      output$particle <- read_parquet(trajec_file)
      output$params <- read_cfg(file.path(simulation_dir, 'CONC.CFG'))
      if (file.exists(error_file)) {
        output$particle_error <- read_parquet(error_file)

        # Update error params
        winderrtf <- (file.exists(winderr_file)) + 2 * file.exists(zierr_file)
        if (winderrtf > 0) {
          config$winderrtf <- winderrtf
        }
      }
    }

    # Exit if not performing footprint calculations
    if (!run_foot) return(output)

    # User defined function to mutate the output object
    output <- before_footprint(output)

    # Unload unnecessary varsiwant columns from memory
    footprint_varsiwant <- c('time', 'indx', 'long', 'lati', 'foot')
    output$particle <- output$particle[ , footprint_varsiwant]

    # Produce footprint --------------------------------------------------------
    # Aggregate the particle trajectory into surface influence footprints. This
    # outputs a netcdf file containing the resultant footprint and various attributes
    foot_id <- ifelse(foot_id == '', '', paste0('_', foot_id))
    foot_file <- file.path(simulation_dir, paste0(simulation_id, foot_id, '_foot.nc'))
    foot <- calc_footprint(output$particle, output = foot_file,
                           receptor = receptor,
                           projection = projection,
                           smooth_factor = smooth_factor,
                           time_integrate = time_integrate,
                           xmn = xmn, xmx = xmx, xres = xres,
                           ymn = ymn, ymx = ymx, yres = yres)

    # Unload trajectories from memory and trigger garbage collection
    rm(output)
    invisible(gc())

    if (is.null(foot)) {
      msg <- 'No non-zero footprint values found within the footprint domain.'
      warning(msg)
      cat(msg, '\n', file = file.path(simulation_dir, 'stilt.log'), append = T)
      return()
    }

    # Symlink footprint to out/footprints
    link_files(foot_file, file.path(output_wd, 'footprints'))

    return(foot)
  })
}
