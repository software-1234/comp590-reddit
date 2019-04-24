#!/bin/sh -v
./hadoop_java.sh DNSLookup
./yarn_mapred.sh DNSLookup /user/elzhou/redditdata/new2 /user/tabathav/output
hdfs dfs -getmerge /user/tabathav/output results
./clean.sh
