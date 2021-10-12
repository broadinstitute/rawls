#!/bin/bash

set -eux

echo "rawls docker/install.sh starting ..."

RAWLS_DIR=$1
cd $RAWLS_DIR

if [ -e "jenkins_env.sh" ]; then
	source "jenkins_env.sh"
fi

# Tests are run in jenkins which has a custom mysql instance just for testing
if [ "$SKIP_TESTS" = "skip-tests" ]; then
	echo skipping tests
else
  echo "starting sbt test ..."
	sbt -J-Xms5g -J-Xmx5g -J-XX:MaxMetaspaceSize=5g test -Dmysql.host=mysql -Dmysql.port=3306 -Duser.timezone=UTC -Dsecrets.skip=true
	echo "sbt test done"
fi

echo "starting sbt assembly ..."
sbt -J-Xms5g -J-Xmx5g -J-XX:MaxMetaspaceSize=5g assembly
echo "... assembly complete, finding and moving jar ..."
RAWLS_JAR=$(find target | grep 'rawls.*\.jar')
mv $RAWLS_JAR .
echo "... jar moved, cleaning ..."
sbt clean

echo "rawls docker/install.sh done"
