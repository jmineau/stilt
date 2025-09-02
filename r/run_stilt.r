#!/usr/bin/env Rscript
# STILT R Executable
# For documentation, see https://jmineau.github.io/stilt/
# Ben Fasoli & James Mineau

# User inputs ------------------------------------------------------------------
project <- '{{project}}'
stilt_wd <- file.path('{{wd}}', project)
output_wd <- file.path(stilt_wd, 'out')
# lib.loc <- .libPaths()[1]  # Use first library location
lib.loc <- NULL  # Use default library location

# Configuration YAML file
config_file <- file.path(stilt_wd, 'config.yaml')

# Parallel simulation settings
n_cores <- 1
n_nodes <- 1
processes_per_node <- n_cores
slurm   <- n_nodes > 1
slurm_options <- list(
  time      = '300:00:00',
  account   = 'lin-np',
  partition = 'lin-np'
)


# Source dependencies ----------------------------------------------------------
setwd(stilt_wd)
source('r/dependencies.r')


# Load configuration YAML file -------------------------------------------------
config <- read_config(config_file)

# Bundle footprint resolutions into a list
if (!is.null(config$xres)) {
  config$xres <- list(config$xres)
}
if (!is.null(config$yres)) {
  config$yres <- list(config$yres)
}

# Bundle varsiwant into a single list
if (!is.null(config$varsiwant)) {
  config$varsiwant <- list(config$varsiwant)
}

if (is.function(config$before_footprint)) {
  # Functions must be wrapped in a list for stilt_apply
  config$before_footprint <- list(config$before_footprint)
}


# Load receptors from CSV file -------------------------------------------------
receptor_file <- config$receptors
receptors <- read.csv(receptor_file, stringsAsFactors = F)
receptors$time <- as.POSIXct(receptors$time, tz = 'UTC')

if ("group" %in% colnames(receptors)) {
  # Group receptors by a common group identifier
  receptors <- receptors %>%
    mutate(group = ifelse(is.na(group),
                          paste0("Point", row_number()),
                          group)) %>%
    group_by(group) %>%
    summarize(
      time = first(time),  # there should be only one time per group
      lati = list(lati),
      long = list(long),
      zagl = list(zagl)) %>%
    ungroup()
}


# Structure out directory ------------------------------------------------------
# Outputs are organized in three formats. by-id contains simulation files by
# unique simulation identifier. particles and footprints contain symbolic links
# to the particle trajectory and footprint files in by-id
for (d in c('by-id', 'particles', 'footprints')) {
  d <- file.path(output_wd, d)
  if (!file.exists(d))
    dir.create(d, recursive = T)
}


# Run trajectory simulations ---------------------------------------------------
args <- list(
  FUN = simulation_step,
  stilt_wd = stilt_wd,
  output_wd = output_wd,
  lib.loc = lib.loc,
  slurm = slurm,
  slurm_options = slurm_options,
  n_cores = n_cores,
  n_nodes = n_nodes,
  processes_per_node = processes_per_node,
  r_time = receptors$time,
  r_lati = receptors$lati,
  r_long = receptors$long,
  r_zagl = receptors$zagl
)
args <- merge_lists(args, config)  # config overrides args
do.call(stilt_apply, args)
