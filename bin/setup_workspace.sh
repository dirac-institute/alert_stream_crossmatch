#! /bin/bash

mkdir -p "../local"
cd "../local"

mkdir -p cutouts
mkdir -p db
mkdir -p old_results

mkdir -p cutouts_debug

cd "../alert_stream_crossmatch"

python db_caching.py 0 "_debug"
# python db_caching.py 0 $1  # command line arg for the suffix

# today="$(date +'%Y%m%d')"
