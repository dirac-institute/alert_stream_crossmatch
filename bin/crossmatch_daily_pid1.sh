#! /bin/bash

today=$(date +"%y%m%d")

./run_crossmatch_simple $today 1 simple 
# running one pid after the other on purpose (get denser historical data from pid2 first)
