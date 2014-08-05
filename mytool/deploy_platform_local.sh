#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
hadoop_version="hadoop-2.4.1"
hadoop_file_local="~/hadoop-common/hadoop-dist/target/${hadoop_version}.tar.gz"
#hadoop_file_remote="~/hadoop_$(date +%Y%m%d%H%M%S).tar.gz"
hadoop_file_remote="~/${hadoop_version}.tar.gz"

function deploy() {
	node=$1
	echo "[$node] Copy files"
	#echo "scp $hadoop_file_local $node:$hadoop_file_remote"
        eval "scp $hadoop_file_local $node:$hadoop_file_remote"
	echo "[$node] Processing files"
	cmd="unlink ~/hadoop; tar zxvf $hadoop_file_remote && ln -s ~/$hadoop_version ~/hadoop && mkdir -p ~/hadoop/conf"
	#echo "ssh $node \'$cmd\'"
	eval ssh $node \"$cmd\" > /dev/null 2&>1
        if [ $? -eq 0 ]; then
            echo "[$node] Complete deployment"
        else
            echo "[$node] Failed to deployment"
        fi
}

echo "Deply Hadoop build files"
for (( i=50; i<=67; i++ )); do
        node="10.113.211.$i"
        (deploy $node) &
done
wait
