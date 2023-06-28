#! /bin/bash
cd /epyc/users/ykwang/Github/dev/alert_stream_crossmatch/bin

today=$(date +"%Y%m%d")
working_dir=/epyc/users/ykwang/Github/dev/alert_stream_crossmatch
data_public=/epyc/data/ztf/alerts/public/ztf_public_$today.tar.gz


source  "/epyc/opt/anaconda-2019/etc/profile.d/conda.sh"
conda activate ztf_kafka
cd $working_dir/alert_stream_crossmatch

./amplitude_outburst_filtering.py $data_public
# ./amplitude_outburst_filtering.py $data_partnership

# mkdir -p $backup_dir/$today/
python combine.py $today
# running one pid after the other on purpose (get denser historical data from pid2 first)
