#! /bin/bash

today=$(date +"%y%m%d")

source  "/epyc/opt/anaconda-2019/etc/profile.d/conda.sh"
conda activate ztf_kafka
cd /epyc/users/ykwang/Github/alert_stream_crossmatch/bin
./run_crossmatch_simple $today 1 simple 
# running one pid after the other on purpose (get denser historical data from pid2 first)
