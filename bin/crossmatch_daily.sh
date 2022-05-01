#! /bin/bash
cd /epyc/users/ykwang/Github/production/alert_stream_crossmatch/bin

. ./setup.cfg
today=$(date +"%y%m%d")

source  "/epyc/opt/anaconda-2019/etc/profile.d/conda.sh"
conda activate ztf_kafka
cd $working_dir/bin

./run_crossmatch_simple $today 2 $db_suffix_partnership
./run_crossmatch_simple $today 1 $db_suffix_partnership

mkdir -p $backup_dir/$today/
cp ../local/db/sqlite_pid2.db $backup_dir/$today/sqlite_pid2.db
cp ../local/db/sqlite_pid2.db /epyc/users/ecbellm/alert_stream_crossmatch/local/db/sqlite2.db
# running one pid after the other on purpose (get denser historical data from pid2 first)
