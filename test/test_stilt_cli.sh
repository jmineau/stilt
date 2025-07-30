#!/bin/bash
# Ben Fasoli
# Integration testing for STILT R wrapper
set -e

# Fetch the tutorial data
[[ -d stilt-tutorials ]] || git clone https://github.com/uataq/stilt-tutorials

chmod +x r/stilt_cli.r

echo "Running r/stilt_cli.r"
r/stilt_cli.r \
  r_time=2015-12-10T00:00:00Z \
  r_lati=40.5 \
  r_long=-112.0 \
  r_zagl=5 \
  met_path=$(pwd)/stilt-tutorials/01-wbb/met \
  met_file_format=%Y%m%d.%Hz.hrrra \
  n_hours=-12 \
  xmn=-113 \
  xmx=-111 \
  xres=0.01 \
  ymn=39.5 \
  ymx=41.5 \
  yres=0.01

# Check output
simulation_id="201512100000_-112_40.5_5"
model_output=$(ls out/by-id/${simulation_id}/${simulation_id}* | wc -l)
if [ $model_output -lt 2 ]; then
  echo "Model output not found."

  echo "stilt.log:"
  cat out/by-id/${simulation_id}/stilt.log
  exit 1
fi

echo "out/by-id/<id> contents:"
ls -lh out/by-id/${simulation_id}

echo "Removing model outputs"
rm -r out/by-id/${simulation_id}
rm out/footprints/${simulation_id}*
rm out/particles/${simulation_id}*

echo "stilt_cli.r test successful"
