#!/bin/bash

# This script runs sbt assembly to produce a target jar file.
# Used by build_jar.sh
#
set -eux

RAWLS_DIR=$1
cd $RAWLS_DIR

export SBT_OPTS="-J-Xms5g -J-Xmx5g -J-XX:MaxMetaspaceSize=5g"
echo "starting sbt clean assembly ..."
sbt 'set assembly / test := {}' clean assembly
echo "... clean assembly complete, finding and moving jar ..."
RAWLS_JAR=$(find target | grep 'sam.*\.jar')
mv $RAWLS_JAR .
echo "... jar moved."
