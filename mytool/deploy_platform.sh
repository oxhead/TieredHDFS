#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
deploy_script="deploy_node.sh"

function deploy() {
	node=$1
	echo "[$node] Copy script"
        eval "scp $DIR/$deploy_script $node:~/"
        echo "[$node] Execute the script"
	echo ssh $node bash $deploy_script
        ssh $node bash $deploy_script
}

for (( i=64; i<=67; i++ )); do
	node="10.113.211.$i"
	deploy $node
done
