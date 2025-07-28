#!/bin/bash
# Ben Fasoli
# Integration testing for STILT R wrapper
set -e

# Fetch the tutorial data
[[ -d stilt-tutorials ]] || git clone https://github.com/uataq/stilt-tutorials

# Replace {{project}} and {{wd}}
sed -i'.bak' -e 's|{{project}}|stilt-test|g' r/run_stilt.r
sed -i'.bak' -e "s|file.path('{{wd}}', project)|getwd()|g" r/run_stilt.r

# Set receptor and footprint information
Rscript r/generate_receptors.r
sed -i'.bak' -e 's|xmn:.*|xmn: -113|' config.yaml
sed -i'.bak' -e 's|xmx:.*|xmx: -111|' config.yaml
sed -i'.bak' -e 's|xres:.*|xres: 0.01|' config.yaml
sed -i'.bak' -e 's|ymn:.*|ymn: 39.5|' config.yaml
sed -i'.bak' -e 's|ymx:.*|ymx: 41.5|' config.yaml

# Set met_path
sed -i'.bak' -e "s|met_path:.*|met_path: 'stilt-tutorials/01-wbb/met'|" config.yaml

# Minimize run duration
sed -i'.bak' -e 's|n_hours:.*|n_hours: -6|' config.yaml

echo "Running r/run_stilt.r"
Rscript r/run_stilt.r

# Check output
model_output=$(ls out/by-id/201512100000_-112_40.5_5/201512100000_-112_40.5_5* | wc -l)
if [ $model_output -lt 2 ]; then
  echo "Model output not found."

  echo "run_stilt.r configuration:"
  cat r/run_stilt.r

  echo "stilt.log:"
  cat out/by-id/201512100000_-112_40.5_5/stilt.log
  exit 1
fi

echo "out/by-id/<id> contents:"
ls -lh out/by-id/201512100000_-112_40.5_5

echo "Removing model outputs"
rm in/receptors.csv
rm out/by-id/201512100000_-112_40.5_5/*
rm out/footprints/*
rm out/particles/*
rm r/run_stilt.r.bak
rm r/config.yaml.bak

echo "run_stilt.r test successful"
