#!/bin/bash

set -e

RAWLS_DIR=$1
cd $RAWLS_DIR

if [ -e "jenkins_env.sh" ]; then
	source "jenkins_env.sh"
fi

# Tests are run in jenkins which has a custom mysql instance just for testing
if [ "$SKIP_TESTS" = "skip-tests" ]; then
	echo skipping tests
else
	sbt -J-Xms5g -J-Xmx5g -J-XX:MaxMetaspaceSize=5g test -Dmysql.host=mysql -Dmysql.port=3306
fi

sbt -J-Xms5g -J-Xmx5g -J-XX:MaxMetaspaceSize=5g assembly
RAWLS_JAR=$(find target | grep 'rawls.*\.jar')
mv $RAWLS_JAR .
sbt clean
