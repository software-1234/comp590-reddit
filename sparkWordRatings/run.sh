#!/bin/sh -v
./spark_javac.sh WordScoreCount
./spark_run_cluster.sh WordScoreCount /user/elzhou/redditdata/new2 /user/tabathav/output
hdfs dfs -getmerge /user/tabathav/output results
./clean.sh
