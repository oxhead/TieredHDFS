#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
build_server="10.113.211.70"

cmd_sync="rsync -r -v -a --exclude=*.tar.gz --exclude=*.html --exclude=*.jar --exclude=*.class ~/hadoop-common $build_server:~/"
cmd_remove="rm -f ~/hadoop-common/hadoop-dist/target/hadoop*.tar.gz"
cmd_build="export JAVA_HOME=/usr/java/jdk1.7.0_10; cd ~/hadoop-common;mvn package -Pdist -DskipTests -Dtar"

echo -e "\nStep 1: sync source code"
eval ${cmd_sync}
echo -e "\nStep 2: remove existing builds"
echo ssh $build_server \'${cmd_remove}\'
eval ssh $build_server \'${cmd_remove}\'
echo -e "\nStep 3: build the source code"
echo ssh $build_server \'${cmd_build}\'
eval ssh $build_server \'${cmd_build}\'
