<p align="center">
  <a href="https://uataq.github.io/stilt/">
    <img src="https://uataq.github.io/stilt/static/img/footprint-circle.png" width=300/>
  </a>
</p>

<h3 align="center">
  Stochastic Time-Inverted Lagrangian Transport model
</h3>

<p align="center">
  An open source Lagrangian particle dispersion model.
</p>

<p align="center">
  <a href="https://github.com/jmineau/stilt/actions?query=branch%3Ajmineau">
    <img src="https://github.com/jmineau/stilt/workflows/Build%20+%20Test/badge.svg"/>
  </a>
  <a href="https://github.com/uataq/stilt/issues">
    <img src="https://img.shields.io/github/issues/uataq/stilt.svg"/>
  </a>
  <a href="https://jmineau.github.io/stilt/">
    <img src="https://img.shields.io/website-up-down-green-red/http/jmineau.github.io/stilt.svg?label=website"/>
  </a>
</p>

## `jmineau` Updates
#### Major Changes
 - YAML based configuration
   - Receptors are now defined in a CSV file
 - Parquet trajectory output
 - Parallelized column/multipoint receptor support
#### Minor Changes
 - Multiple footprints can be generated per simulation for each resolution
 - `output_wd` is no longer removed, instead simulations are overwritten if the same `simulation_id` is used
 - `simulation_id` for MultiPoint receptors are now generated based on a md5 hash of the receptor locations WKT
   - Would recommend users specify their own simulation ID for MultiPoint receptors
   - Other ideas for generating simulation IDs are welcome :)
 - 'traj' is now 'trajec' for clarity
 - Updated tests including updating docker image to use `debian:bookworm-slim`
 - Bug fixes and docs & default updates
#### Changelog
- 2025-09-01 : Removed `foot_id` in favor of only creating multiple footprints for multiple resolutions
  - The footprint file structure is now '<simulation_id>_\<xres>x\<yres>_foot.nc'
  - `simulation_step` will return a single footprint if only one resolution is specified, otherwise a named list of footprints is returned.
- 2025-08-27 : Write receptor information to a csv for each simulation
- 2025-08-19 : Use WKT representation of multipoint locations to generate cross-platform md5 hash
- 2025-07-29 : Updated docker image to use `debian:bookworm-slim`. This change ensures that the Docker image is built on a more recent and secure base image, which may include important security updates and performance improvements. Also, docker couldn't find an available version of `purrr` that worked with `arrow` on `debian:bullseye-slim`. This change resolves that issue.
- 2025-07-26 : Major changes to the input configuration system.
  - The configuration file is now a YAML file named `config.yaml` located in the working directory. This file contains all the necessary parameters for running STILT simulations, including model settings, receptor information, and trajectory options.
  - A corresponding simulation config yaml file is generated in each simulation directory, which includes all the parameters used for that specific simulation. This ensures reproducibility and allows users to easily review or share their simulation settings. We no longer write a `config.json` file. As long as the input parameters are valid, the config yaml file is written first and can be used to identify simulation directories.
  - receptors are now specified in a csv file. A helper function is provided to generate a csv for a grid of receptors. The inclusion of a `group` column allows for grouping of receptors, which means Column/MultiPoint receptors can now be passed to slurm
  - Created a custom receptor object that is aware of the kind of receptor it is (e.g., Point, Column, MultiPoint, etc.).
  - Updated calculation of `xhgt` (original receptor height) to be correct for MultiPoint receptors. Still needs to be updated for mixed & multi-column receptors.
  - Simulation IDs for non Point/Column receptors that are NA are not set based on a md5 hash of the receptor locations. Not human-readable, but unique. Probably recommended that users specify their own simulation ID for these cases. Smart ID templates have been removed due to the complexity of non-Point receptors.
  - Removed `before_trajec`. There is essentially nothing to be manipulated before the trajectory is run that could not be set in the configuration file.
  - `nbptyp` was missing from the `run_stilt.r` file. It is now included in the configuration file and passed to the model.
  - Removed `validate_footprint_extent.r` as stilt can be run without outputting a footprint. The grid extent is only needed if a footprint is generated or if meteorology subgrids are enabled.
  - rundir has been renamed to simulation_dir to reduce confusion. Simularily, the run_time has been renamed to receptor_time (r_time) or simply time.
  - Changed default `numpar` to 1000. This is a common values for many applications and should be sufficient for most users. Users can still override this value in the configuration file
- 2025-06-23 : Updated `varsiwant` to match current hysplit document. Additionally, included `pres` as a default `varsiwant` variable. This change ensures that the pressure variable is always included in the trajectory output, which is important for many atmospheric modeling applications.
- 2025-06-19 : Added `foot_id` option. This allows for users to specify an optional identifier for the footprint output files. This can be useful for distinguishing between different footprints generated using the same trajectory data (eg. different grid resolutions). The footprint files will now be named `<simulation_id>_<foot_id>_foot.nc`, where `foot_id` is the specified identifier.
- 2025-06-19 : Replaced 'traj' with 'trajec' in all instances to be more clear and consistent with the full word 'trajectory'. This change improves clarity in the codebase and aligns with the terminology used in the documentation.
- 2025-06-18 : Added support for smart `simulation_id` templates. Users can now specify a template string with curly-brace placeholders (e.g., `myrun_{lati}_{long}_{zagl}_{run_time}`) that will be filled in with the actual values for each simulation.
- 2025-06-18 : Removed `simulation_id` from `write_output` and `read_output` parameters. It is assumed that the simulation ID is always the same as the `rundir` name. As the rundir is within the 'by-id' directory.
- 2025-06-18 : Removed the `write_trajec` option. Trajectory output is now always written if `run_trajec` is TRUE. If `run_trajec` is FALSE, it is assumed the trajectory file already exists and will be loaded. This simplifies configuration and ensures consistent output behavior.
- 2025-06-18 : Removed `reset_output_wd` option. The model now never removes the output directory. However, if a simulation with the same simulation ID is run, the model will overwrite the existing output files. This change simplifies the workflow and avoids confusion about output directory management. Users, therefore, need to manage output directories themselves if they want to separate outputs for different simulations.
- 2024-10-31 : Added `reset_output_wd` option to reset the output directory to the working directory. Previously, the output_wd would be reset whenever 'run_trajec' was True. However, one might want to run multiple simulations with the same output directory. This option allows for that.
- 2024-10-02 : Restructured output. Simulation configuration information is stored neatly in a json, including receptor information. Trajectory output is now stored as parquet files, which allows languages other than `R` to read them. Additionally added `write_trajec` option to disable trajectory output. Added receptor information to the footprint netcdf attributes, including run_time.
- 2024-09-14 : Set `lib.loc <- NULL` to allow for auto-selection of the library path and to use site libraries in HPC environments

## Docs

[**STILT documentation**](https://uataq.github.io/stilt/)  
[Methods details](https://www.geosci-model-dev.net/11/2813/2018/)

## About

STILT would not be possible without the strong community of developers behind it. This distribution contains a completely redesigned STILT wrapper and proposes a centralized, collaborative platform for documentation and future development. Model development in the form of feature enhancements, documentation updates, bug fixes, or simple suggestions from the community are welcome. Contribution guidelines can be found [here](https://uataq.github.io/stilt/#/contribute).

### Relevant manuscripts

Loughner, C. P., Fasoli, B., Stein, A. F., Lin, J. C.: Incorporating features from the Stochastic Time-Inverted Lagrangian Transport (STILT) model into the Hybrid Single-Particle Lagrangian Integrated Trajectory (HYSPLIT) model: a unified dispersion model for time-forward and time-reversed applications, J. Appl. Meteorol. Climatol., [10.1175/JAMC-D-20-0158.1](https://doi.org/10.1175/JAMC-D-20-0158.1), 2021.

Fasoli, B., Lin, J. C., Bowling, D. R., Mitchell, L., and Mendoza, D.: Simulating atmospheric tracer concentrations for spatially distributed receptors: updates to the Stochastic Time-Inverted Lagrangian Transport model's R interface (STILT-R version 2), Geosci. Model Dev., [10.5194/gmd-11-2813-2018](https://doi.org/10.5194/gmd-11-2813-2018), 2018.

Stein, A. R., Draxler, R. R., Rolph, G. D., Stunder, B. J. B., and Cohen M. D.: NOAA’s HYSPLIT atmospheric transport and dispersion modeling system. Bull. Amer. Meteor. Soc., [10.1175/BAMS-D-14-00110.1](https://doi.org/10.1175/BAMS-D-14-00110.1), 2015.

Lin, J. C., Gerbig, C., Wofsy, S. C., Andrews, A. E., Daube, B. C., Davis, K. J. and Grainger, C. A.: A near-field tool for simulating the upstream influence of atmospheric observations: The Stochastic Time-Inverted Lagrangian Transport (STILT) model, J. Geophys. Res., [10.1029/2002JD003161](https://doi.org/10.1029/2002JD003161), 2003.
