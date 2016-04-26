#!/bin/bash

set -e

RAWLS_DIR=$1
cd $RAWLS_DIR

if [ -e "jenkins_env.sh" ]; then
	source "jenkins_env.sh"
fi

# catch sbt issues separately
sbt update && echo "sbt updated successfully."

# Tests are run in jenkins which has a custom mysql instance just for testing
sbt -J-Xms4g -J-Xmx4g test -Dmysql.host=localhost -Dmysql.port=3310
sbt -J-Xms4g -J-Xmx4g assembly
RAWLS_JAR=$(find target | grep 'rawls.*\.jar')
mv $RAWLS_JAR .
sbt clean
