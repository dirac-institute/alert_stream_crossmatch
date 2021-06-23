#! /bin/bash

today=$(date +"%y%m%d")

source  "/epyc/opt/anaconda-2019/etc/profile.d/conda.sh"
conda activate ztf_kafka
cd /epyc/users/ykwang/Github/production/alert_stream_crossmatch/bin

./run_crossmatch_simple $today 2 _pid2
./run_crossmatch_simple $today 1 _pid2


mkdir -p ~/data/ztf_db_backups/$today/
cp ../local/db/sqlite_pid2.db ~/data/backups/$today/sqlite_pid2.db
# running one pid after the other on purpose (get denser historical data from pid2 first)
