#! /bin/bash
for d in {190701..190731}; do
    for program in 2 1; do
         ./run_crossmatch_archival $d $program _archival
    done
done
