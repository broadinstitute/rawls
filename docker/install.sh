#!/bin/bash

set -e

RAWLS_DIR=$1
cd $RAWLS_DIR
sbt assembly
RAWLS_JAR=$(find target | grep 'rawls.*\.jar')
mv RAWLS_JAR .
sbt clean
