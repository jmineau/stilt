## STILT configuration

STILT is configured using a combination of a YAML file (`config.yaml`) and the main execution script (`run_stilt.r`).

- **Most model, footprint, meteorological, transport, and error parameters are set in `config.yaml`.**
- **System and parallelization settings (such as project directory, output directory, and SLURM options) are set directly in `run_stilt.r`.**


### System configuration
These parameters are set directly in the `run_stilt.r` script, not in `config.yaml`:

| Arg         | Description                                                                                     |
| ----------- | ----------------------------------------------------------------------------------------------- |
| `project`   | Project name. Defaults to the name of the directory specified in `stilt_init()`          |
| `stilt_wd`  | Root directory of the STILT project. Defaults to the directory created by `stilt_init()` |
| `output_wd` | Directory containing simulation output files. Defaults to `<stilt_wd>/out/`                     |
| `lib.loc`   | Path to installed R packages, passed to `library()`                                             |

### Parallel simulation settings
These parameters are set directly in the `run_stilt.r` script, not in `config.yaml`:

| Arg                  | Description                                                                                                                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `n_nodes`            | If using SLURM for job submission, number of nodes to utilize                                                                                                                             |
| `n_cores`            | Number of cores per node to parallelize simulations by receptor locations and times                                                                                                       |
| `processes_per_node` | Number of processes to run on each node. Can be set higher than n_cores for nodes which support [hyperthreading](https://scicomp.ethz.ch/wiki/Using_hyperthreading)                       |
| `slurm`              | Logical indicating the use of rSLURM to submit job(s). When using SLURM, a `<stilt_wd>/_rslurm` directory is created to contain the SLURM submission scripts and node-specific log files. |
| `slurm_options`      | Named list of options passed to `sbatch` using `rslurm::slurm_apply()`. This typically includes `time`, `account`, and `partition` values                                                 |

### Receptor placement

Receptors are specified in a CSV file, by default located at `<stilt_wd>/in/receptors.csv`. The csv file must contain the columns `time`, `lati`, `long`, and `zagl`.
> A helper function `generate_receptors()` is provided and can be used to generate a CSV for a grid of receptors.

The CSV file is read into a data frame where each row corresponds to a receptor in space and time. Receptors are distributed to their own simulation. An additional column, `group`, can be used to group receptors together to create Column or MultiPoint receptors.

A custom `receptor` object is created for each receptor, which is aware of its kind (e.g., Point, Column, MultiPoint). This allows for more flexible handling of different receptor types in the model.

```r
receptor <- create_receptor(
  time = as.POSIXct("2015-12-10 00:00:00"),
  lati = 40.5,
  long = -112.0,
  zagl = 5
)

str(receptor)
# List of 3
#  $ time     : POSIXct[1:1], format: "2015-12-10"
#  $ kind     : chr "Point"
#  $ locations: tibble [1 × 3] (S3: tbl_df/tbl/data.frame)
#   ..$ lati: num 40.5
#   ..$ long: num -112
#   ..$ zagl: num 5
```

The different kinds of receptors are summarized in the table below:

| Kind       | Description                                                                                                                                                                           |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Point      | Single receptor at a specific time and location                                                                                                                                       |
| Column     | A vertical column receptor at a specific time and lati/long between two zagl heights. Particles are distributed evenly between the two zagl heights                                   |
| MultiPoint | Multiple point receptors at a specific time at different locations. Particles are divided evenly between the receptors. This can be useful for representing a *slant* column receptor |

> Mixing of Point and Column receptors as well as MultiColumn receptors are not currently supported in this version of STILT, but are possible through HYSPLIT and can be implemented in the future. Additionally, the idea of an *area* receptor exists, but has not been implemented in any R version of STILT.

### Footprint calculation methods

| Arg              | Description                                                                                                                                                                                                                                                                                                                                     |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hnf_plume`      | logical indicating whether to apply a vertical gaussian plume model to rescale the effective dilution depth for particles in the hyper near-field. This acts to scale up the influence of hyper-local fluxes on the receptor. If enabled, requires `varsiwant` to include a minimum of `dens, tlgr, sigw, foot, mlht, samt`. Default is enabled |
| `projection`     | [proj4](https://proj4.org/usage/quickstart.html) string defining the map projection of the footprint netCDF output. Defaults to `+proj=longlat`                                                                                                                                                                                                 |
| `smooth_factor`  | factor by which to linearly scale footprint smoothing. Defaults to 1.                                                                                                                                                                                                                                                                           |
| `time_integrate` | logical indicating whether to integrate footprint over time or retain discrete hourly time steps in footprint output                                                                                                                                                                                                                            |
| `xmn`            | grid start longitude, in degrees from -180 to 180                                                                                                                                                                                                                                                                                               |
| `xmx`            | grid end longitude, in degrees from -180 to 180                                                                                                                                                                                                                                                                                                 |
| `ymn`            | grid start latitude, in degrees from -180 to 180                                                                                                                                                                                                                                                                                                |
| `ymx`            | grid end latitude, in degrees from -180 to 180                                                                                                                                                                                                                                                                                                  |
| `xres`           | resolution for longitude grid, in projection units (degrees for lat/lon, meters for other). Multiple resolutions can be specified as a vector which will be paired element-wise with `yres`.                                                                                                                                                                                                                                               |
| `yres`           | resolution for latitude grid, in projection units (degrees for lat/lon, meters for other). Multiple resolutions can be specified as a vector which will be paired element-wise with `xres`.                                                                                                                                                                                                                                                       |

### Meteorological data input

| Arg                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `met_path`           | Absolute path to ARL compatible meteorological data files                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `met_file_format`    | String detailing file naming convention for meteorological data files using a mixture of datetime and regex syntax. The formatting string accepts `grep` compatible regular expressions (`.\*.arl`), `strftime` compatible datetime strings (`%Y%m%d%H`) or any combination of the two. Datetime syntax is expanded to all unique combinations required for the receptor and simulation duration and the intersection between the requested files and files available in `met_path` is determined with `grep`, allowing partial matching and compatible regular expressions to be used to identify the relevant data. Matching does not require the full format to be specified - e.g. `\*.arl`, `%Y`, `%Y%m%d`, `%Y%m%d_d0.*.arl` would all match with a file named `20180130_d01.arl`. |
| `met_file_tres` | Time resolution of meteorological data files. To determine the time resolution in an ARL compatible meteorological data file, refer to the README, including the file naming convention, provided by the data source. For example, the [NOAA HRRR README](https://www.ready.noaa.gov/data/archives/hrrr/README.TXT) specifies a "6 hour data file beginning with 00z - 05z in the first file of the day". Defaults to '6 hours'                                                                                                                                                                                                                                                                                                                                                               |
| `met_subgrid_buffer` | Percent to extend footprint area for meteorological subdomain when using `met_subgrid_enable`. Defaults to 0.1 (10%)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `met_subgrid_enable` | Enables extraction of spatial subdomains from files in `met_path` using HYSPLIT's `xtrct_grid` binary prior to executing simulations. If enabled, will create files in `<output_wd>/met/`. This can substantially accelerate simulation speed at the cost of increased disk usage. Defaults to disabled                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `met_subgrid_levels` | If set, extracts the defined number of vertical levels from the meteorological data files to further accelerate simulations. Defaults to `NA`, which includes all vertical levels available                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `n_met_min`          | Require a minimum number of meteorological data files to be matched by `met_file_format` for the simulation to proceed. Useful for handling periods where meteorological data may be missing. For a -24 hour simulation using the 6 hour HRRR met data files, `n_met_min` should be set to 5. Defaults to 1.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |

> NOAA publishes High Resolution Rapid Refresh (HRRR) mesoscale model data in the ARL packed format required for STILT at [ftp://arlftp.arlhq.noaa.gov/pub/archives/hrrr/](ftp://arlftp.arlhq.noaa.gov/pub/archives/hrrr/). This is often the easiest place to start but is only available after June 15, 2015. The coupling of the popular Weather Research and Forecasting (WRF) model with STILT is well documented by [Nehrkorn, 2010](https://link.springer.com/article/10.1007%2Fs00703-010-0068-x). You can access various compatible gridded meteorological data products at [https://www.ready.noaa.gov/archives.php](https://www.ready.noaa.gov/archives.php).

### Model control

| Arg             | Description                                                                                                                                                                                                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `n_hours`       | Number of hours to run each simulation; negative indicates backward in time                                                                                                                                                                                                     |
| `numpar`        | number of particles to be run; defaults to 1000                                                                                                                                                                                                                                 |
| `rm_dat`        | Logical indicating whether to delete `PARTICLE.DAT` after each simulation. Default to TRUE to reduce disk space since all of the trajectory information is also stored in trajectory parquet file alongside the calculated upstream influence footprint                         |
| `run_foot`      | Logical indicating whether to produce footprints. If FALSE, `run_trajec` must be TRUE. This can be useful when calculating trajectories separate from footprints                                                                                                                |
| `run_trajec`    | Logical indicating whether to produce new trajectories with `hycs_std`. If FALSE, will try to load the previous trajectory outputs. This is often useful for regridding purposes                                                                                                |
| `simulation_id` | Unique identifier for each simulation; defaults to NA which determines a unique identifier for each simulation by hashing the time and receptor location                                                                                                                                |
| `timeout`       | number of seconds to allow `hycs_std` to complete before sending SIGTERM and moving to the next simulation; defaults to 3600 (1 hour)                                                                                                                                           |
| `varsiwant`     | character vector of 4-letter `hycs_std` variables. Defaults to the minimum required variables including `'time', 'indx', 'long', 'lati', 'zagl', 'foot', 'mlht', 'pres', 'dens', 'samt', 'sigw', 'tlgr'`. Can optionally include options listed below.                                  |

#### Available `varsiwant` arguments

- `crai` convective rainfall rate [m/min]
- `dens` air density [kg/m3]
- `dmas` particle weight changes due to mass violation in wind fields [initial value = 1.0]
- `dswf` downward shortwave radiation [W/m2]
- `foot` footprint, or sensitivity of mixing ratio to surface fluxes [ppm/(μmole/m2/s)]
- `icdx` cloud index when using RAMS (Grell scheme) [1=updraft,2=environment,3=downdraft]
- `indx` unique particle identifier
- `lati` latitude position of particle [degrees]
- `lcld` low cloud cover [%]
- `long` longitude position of particle [degrees]
- `mlht` mixed-layer height [m]
- `pres` pressure at particle's vertical position [hPa]
- `rain` total rainfall rate [m/min]
- `rhfr` relative humidity fraction [0~1.0]
- `samt` amount of time particle spends below VEGHT (see section on SETUP.CFG) [min]
- `shtf` sensible heat flux [W/m2]
- `sigw` standard deviation of vertical velocity; measure of strength of vertical turbulence [m/s]
- `sphu` specific humidity [g/g]
- `tcld` total cloud cover [%]
- `temp` air temperature at lowest model layer [K]
- `temz` temperature at particle's vertical position [K]
- `time` time since start of simulation; negative if going backward in time [min] indx particle index
- `tlgr` Lagrangian decorrelation timescale [s]
- `whtf` latent heat flux [W/m2]
- `wout` vertical mean wind [m/s]
- `zagl` vertical position of particle [m above ground level]
- `zfx1` vertical displacement due to convective flux [m]
- `zloc` limit of convection heights [m]
- `zsfc` terrain height [m above sea level]

### Transport and dispersion

| Arg           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `w_option`    | vertical motion calculation method. 0: use vertical velocity from data, 1: isob, 2: isen, 3: dens, 4: sigma; defaults to 0                                                                                                                                                                                                                                                                                                                   |
| `z_top`       | top of model domain, in meters above ground level; defaults to 25000.0                                                                                                                                                                                                                                                                                                                                                                       |
| `zicontroltf` | flag that specifies whether to scale the PBL heights in STILT uniformly in the entire model domain; defaults to 0. If set to 1, then STILT looks for a file called "ZICONTROL" that specifies the scaling for the PBL height. The first line indicates the number of hours that the PBL height will be changed, and each subsequent line indicates the scaling factor for that hour                                                          |
| `ziscale`     | manually scale the mixed-layer height, with each element specifying a scaling factor for each simulation hour (ziscale can be of length that is smaller than abs(nhrs). A vector can be passed as a list (e.g. `ziscale <- list(rep(0.8, 24))` scales the mixed layer height to 80% for the first 24 hours of all simulations) or a list of vectors specific to each simulation (e.g. `ziscale <- rep(list(rep(0.8, 24)), nrow(receptors))`) |

> Additional arguments can be referenced in the [HYSPLIT user's guide](https://www.arl.noaa.gov/documents/reports/hysplit_user_guide.pdf)

### Transport error calculations

| Arg           | Description                                                          |
| ------------- | -------------------------------------------------------------------- |
| `siguverr`    | standard deviation of horizontal wind errors [m/s]                   |
| `tluverr`     | standard deviation of horizontal wind error timescale [min]          |
| `zcoruverr`   | vertical correlation lengthscale [m]                                 |
| `horcoruverr` | horizontal correlation lengthscale [km]                              |
| `sigzierr`    | standard deviation of mixed layer height errors [%]                  |
| `tlzierr`     | standard deviation of mixed layer height timescale [min]             |
| `horcorzierr` | horizontal correlation lengthscale of mixed layer height errors [km] |

### Inject user defined functions

This is an advanced option that can be used to inject user code into the simulation flow. This can be used to add custom logic at key points in the code, such as correcting for [mass violation in the input wind fields](https://github.com/uataq/stilt/issues/41#issuecomment-656174491).

| Arg                | Description                                                                                                                                                                                                                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `before_footprint` | function or path to function that returns the `output` object and is executed prior to calculating the gridded fotprint but after calculating the ensemble's particle trajectories; see [correcting for meteorological mass violation](https://github.com/uataq/stilt/issues/41#issuecomment-656174491) for an example |

---

## Next steps

- [Execution](execution.md) for details on how to run your STILT simulations
- [Tutorial: Stationary simulations](https://github.com/uataq/stilt-tutorials/tree/main/01-wbb)
