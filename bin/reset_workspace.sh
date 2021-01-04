#! /bin/bash

local_dir="/epyc/users/ykwang/Github/alert_stream_crossmatch/local"
cd $local_dir

today="$(date +'%Y%m%d')"

mv db "db_${today}" 
mv cutouts "cutouts_${today}"

mv "db_${today}" old_results/old_dbs
mv "cutouts_${today}" old_results/old_cutouts

mkdir cutouts
mkdir db

cd -
