#! /bin/bash

today=$(date +"%y%m%d")

./run_crossmatch_simple $today 2 _pid2
./run_crossmatch_simple $today 1 _pid2
# running one pid after the other on purpose (get denser historical data from pid2 first)
