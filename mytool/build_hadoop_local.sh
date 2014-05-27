#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo -e "Build the source code"
mvn package -Pdist -DskipTests -Dtar
