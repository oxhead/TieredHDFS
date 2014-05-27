#!/bin/bash

build_server="10.113.211.70"
hadoop_version="hadoop-2.4.1-SNAPSHOT"
hadoop_file_remote="~/hadoop-common/hadoop-dist/target/${hadoop_version}.tar.gz"
hadoop_file_local="~/hadoop_$(date +%Y%m%d%H%M%S).tar.gz"


# 1) Download the latested build
cmd="scp $build_server:$hadoop_file_remote $hadoop_file_local"
eval $cmd > /tmp/download.log 2>&1

# 2) Prepare directories
eval "unlink ~/hadoop"
eval "rm -rf ~/${hadoop_version}"
cmd="tar zxvf $hadoop_file_local -C ~/ && ln -s ~/${hadoop_version} ~/hadoop && mkdir ~/hadoop/conf"
eval $cmd > /tmp/link.log 2>&1
