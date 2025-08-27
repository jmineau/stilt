#!/usr/bin/env Rscript
# Generate receptors for STILT
# To be run from the STILT project working directory
# For documentation, see https://jmineau.github.io/stilt/
# Ben Fasoli & James Mineau

# Output file for receptors
receptor_out_file <- 'in/receptors.csv'

# Receptor timing, yyyy-mm-dd HH:MM:SS (UTC)
t_start <- '2015-12-10 00:00:00'
t_end   <- '2015-12-10 00:00:00'
receptor_times <- seq(from = as.POSIXct(t_start, tz = 'UTC'),
                 to   = as.POSIXct(t_end, tz = 'UTC'),
                 by   = 'hour')

# Receptor location(s)
lati <- 40.5
long <- -112.0
zagl <- 5

# Expand the times, latitudes, and longitudes to form the unique receptors
# that are used for each simulation
receptors <- expand.grid(time = receptor_times, lati = lati, long = long,
                         zagl = zagl, KEEP.OUT.ATTRS = F, stringsAsFactors = F)

# Write receptors to CSV
dir.create(dirname(receptor_out_file), showWarnings = FALSE, recursive = TRUE)
write.csv(receptors, receptor_out_file, row.names = F, quote = F)