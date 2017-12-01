#!/bin/bash

set -e

RAWLS_DIR=$1
cd $RAWLS_DIR

if [ -e "jenkins_env.sh" ]; then
	source "jenkins_env.sh"
fi

# Tests are run in jenkins which has a custom mysql instance just for testing
#sbt -J-Xms4g -J-Xmx4g -J-XX:MaxMetaspaceSize=1024m test -Dmysql.host=mysql -Dmysql.port=3306 -Ddirectory.url=ldap://opendj:389 -Ddirectory.password=testtesttest
sbt -J-Xms4g -J-Xmx4g -J-XX:MaxMetaspaceSize=1024m assembly
RAWLS_JAR=$(find target | grep 'rawls.*\.jar')
mv $RAWLS_JAR .
sbt clean
