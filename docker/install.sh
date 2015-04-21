#!/bin/bash

set -e

RAWLS_DIR=$1
cd $RAWLS_DIR
sbt assembly
SNOOP_JAR=$(find target | grep 'rawls.*\.jar')
mv $RAWLS_DIR .
sbt clean
