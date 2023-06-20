#! /bin/bash

find /epyc/data/ztf/alerts/public/ztf_public*.gz -size +10M > alert_list.txt
find /epyc/data/ztf/alerts/partnership/ztf_partnership*.gz -size +10M >> alert_list.txt
