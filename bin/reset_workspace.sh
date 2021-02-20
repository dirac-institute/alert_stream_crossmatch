#! /bin/bash

cd "../local"

today="$(date +'%Y%m%d')"

mv db "db_${today}" 
mv cutouts "cutouts_${today}"

mv "db_${today}" old_results/old_dbs
mv "cutouts_${today}" old_results/old_cutouts

mkdir cutouts
mkdir db

cd "../alert_stream_crossmatch"
python db_caching.py "y" $1 
