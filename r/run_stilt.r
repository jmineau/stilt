#!/usr/bin/env Rscript
# STILT R Executable
# For documentation, see https://uataq.github.io/stilt/
# Ben Fasoli

# Read input configuration -----------------------------------------------------

# Need to load the yaml package before other dependencies
lib.loc <- NULL  # initially set to NULL
if (!require(yaml, character.only = T, lib.loc = lib.loc)) {
  install.packages('yaml', repo = 'https://cran.rstudio.com/', lib = lib.loc)
  require(yaml, character.only = T, lib.loc = lib.loc)
}

# Read the configuration file
config <- yaml.load_file('config.yaml')

# Read in.csv
in_csv_file <- 'in/in.csv'
in_csv <- read.csv(, stringsAsFactors = F)
if (nrow > 0) {
  # Replace config values with simulation-specific values from in.csv
  for (col in colnames(in_csv)) {
    config[[col]] <- in_csv[[col]]
  }
}


# User inputs ------------------------------------------------------------------
project <- config$project
stilt_wd <- config$stilt_wd
output_wd <- config$output_wd
lib.loc <- config$lib_loc

# Parallel simulation settings
n_cores <- config$n_cores
n_nodes <- config$n_nodes
slurm <- config$slurm & n_cores > 1
slurm_options <- config$slurm_options

# Expand the run times, latitudes, and longitudes to form the unique receptors
# that are used for each simulation
receptors <- expand.grid(run_time = run_times, lati = receptors$lati, long = receptors$long,
                         zagl = receptors$zagl, KEEP.OUT.ATTRS = F, stringsAsFactors = F)

# Footprint grid settings, must set at least xmn, xmx, ymn, ymx below
hnf_plume <- config$hnf_plume
projection <- config$projection
smooth_factor <- config$smooth_factor
time_integrate <- config$time_integrate
xmn <- config$xmn
xmx <- config$xmx
ymn <- config$ymn
ymx <- config$ymx
xres <- config$xres
yres <- config$yres

# Meteorological data input
met_path <- config$met_path
met_file_format <- config$met_file_format
met_subgrid_buffer <- config$met_subgrid_buffer
met_subgrid_enable <- config$met_subgrid_enable
met_subgrid_levels <- config$met_subgrid_levels
n_met_min <- config$n_met_min

# Model control
n_hours <- config$n_hours
numpar <- config$numpar
rm_dat <- config$rm_dat
run_foot <- config$run_foot
run_trajec <- config$run_trajec
simulation_id <- config$simulation_id
timeout <- config$timeout
varsiwant <- config$varsiwant

# Transport and dispersion settings
capemin <- config$capemin
cmass <- config$cmass
conage <- config$conage
cpack <- config$cpack
delt <- config$delt
dxf <- config$dxf
dyf <- config$dyf
dzf <- config$dzf
efile <- config$efile
emisshrs <- config$emisshrs
frhmax <- config$frhmax
frhs <- config$frhs
frme <- config$frme
frmr <- config$frmr
frts <- config$frts
frvs <- config$frvs
hscale <- config$hscale
ichem <- config$ichem
idsp <- config$idsp
initd <- config$initd
k10m <- config$k10m
kagl <- config$kagl
kbls <- config$kbls
kblt <- config$kblt
kdef <- config$kdef
khinp <- config$khinp
khmax <- config$khmax
kmix0 <- config$kmix0
kmixd <- config$kmixd
kmsl <- config$kmsl
kpuff <- config$kpuff
krand <- config$krand
krnd <- config$krnd
kspl <- config$kspl
kwet <- config$kwet
kzmix <- config$kzmix
maxdim <- config$maxdim
maxpar <- ifelse(is.null(config$maxpar), numpar, config$maxpar)
mgmin <- config$mgmin
mhrs <- config$mhrs
nbptyp <- config$nbptyp
ncycl <- config$ncycl
ndump <- config$ndump
ninit <- config$ninit
nstr <- config$nstr
nturb <- config$nturb
nver <- config$nver
outdt <- config$outdt
p10f <- config$p10f
pinbc <- config$pinbc
pinpf <- config$pinpf
poutf <- config$poutf
qcycle <- config$qcycle
rhb <- config$rhb
rht <- config$rht
splitf <- config$splitf
tkerd <- config$tkerd
tkern <- config$tkern
tlfrac <- config$tlfrac
tout <- config$tout
tratio <- config$tratio
tvmix <- config$tvmix
veght <- config$veght
vscale <- config$vscale
vscaleu <- config$vscaleu
vscales <- config$vscales
wbbh <- config$wbbh
wbwf <- config$wbwf
wbwr <- config$wbwr
wvert <- config$wvert
w_option <- config$w_option
zicontroltf <- config$zicontroltf
ziscale <- rep(config$ziscale, nrow(receptors))
z_top <- config$z_top

# Transport error settings
horcoruverr <- config$horcoruverr
siguverr <- config$siguverr
tluverr <- config$tluverr
zcoruverr <- config$zcoruverr

horcorzierr <- config$horcorzierr
sigzierr <- config$sigzierr
tlzierr <- config$tlzierr

# Source custom functions 
# - Includes before_trajec and before_footprint functions to manipulate the simulation output
rsc <- dir(file.path(stilt_wd, 'r', 'custom_src'), pattern = '.*\\.r$', full.names = T)
invisible(lapply(rsc, source))

# Source dependencies ----------------------------------------------------------
setwd(stilt_wd)
source('r/dependencies.r')


# Structure out directory ------------------------------------------------------
# Outputs are organized in three formats. by-id contains simulation files by
# unique simulation identifier. particles and footprints contain symbolic links
# to the particle trajectory and footprint files in by-id
system(paste0('rm -r ', output_wd, '/footprints'), ignore.stderr = T)
if (run_trajec) {
  system(paste0('rm -r ', output_wd, '/by-id'), ignore.stderr = T)
  system(paste0('rm -r ', output_wd, '/met'), ignore.stderr = T)
  system(paste0('rm -r ', output_wd, '/particles'), ignore.stderr = T)
}
for (d in c('by-id', 'particles', 'footprints')) {
  d <- file.path(output_wd, d)
  if (!file.exists(d))
    dir.create(d, recursive = T)
}


# Run trajectory simulations ---------------------------------------------------
stilt_apply(FUN = simulation_step,
            simulation_id = simulation_id,
            slurm = slurm, 
            slurm_options = slurm_options,
            n_cores = n_cores,
            n_nodes = n_nodes,
            before_footprint = list(before_footprint),
            before_trajec = list(before_trajec),
            lib.loc = lib.loc,
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
            hnf_plume = hnf_plume,
            horcoruverr = horcoruverr,
            horcorzierr = horcorzierr,
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
            met_file_format = met_file_format,
            met_path = met_path,
            met_subgrid_buffer = met_subgrid_buffer,
            met_subgrid_enable = met_subgrid_enable,
            met_subgrid_levels = met_subgrid_levels,
            mgmin = mgmin,
            n_hours = n_hours,
            n_met_min = n_met_min,
            ncycl = ncycl,
            ndump = ndump,
            ninit = ninit,
            nstr = nstr,
            nturb = nturb,
            numpar = numpar,
            nver = nver,
            outdt = outdt,
            output_wd = output_wd,
            p10f = p10f,
            pinbc = pinbc,
            pinpf = pinpf,
            poutf = poutf,
            projection = projection,
            qcycle = qcycle,
            r_run_time = receptors$run_time,
            r_lati = receptors$lati,
            r_long = receptors$long,
            r_zagl = receptors$zagl,
            rhb = rhb,
            rht = rht,
            rm_dat = rm_dat,
            run_foot = run_foot,
            run_trajec = run_trajec,
            siguverr = siguverr,
            sigzierr = sigzierr,
            smooth_factor = smooth_factor,
            splitf = splitf,
            stilt_wd = stilt_wd,
            time_integrate = time_integrate,
            timeout = timeout,
            tkerd = tkerd,
            tkern = tkern,
            tlfrac = tlfrac,
            tluverr = tluverr,
            tlzierr = tlzierr,
            tout = tout,
            tratio = tratio,
            tvmix = tvmix,
            varsiwant = list(varsiwant),
            veght = veght,
            vscale = vscale,
            vscaleu = vscaleu,
            vscales = vscales,
            w_option = w_option,
            wbbh = wbbh,
            wbwf = wbwf,
            wbwr = wbwr,
            wvert = wvert,
            xmn = xmn,
            xmx = xmx,
            xres = xres,
            ymn = ymn,
            ymx = ymx,
            yres = yres,
            zicontroltf = zicontroltf,
            ziscale = ziscale,
            z_top = z_top,
            zcoruverr = zcoruverr)
