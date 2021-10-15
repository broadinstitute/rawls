#!/bin/bash

set -eu

ENV=$1
SQLFILE=$2

echo "rawls sqlrunner.sh starting ..."

REALPATH="$(cd ${SQLFILE%/*}; pwd)/${SQLFILE##*/}"

echo "using environment $ENV ..."
echo "using SQL file $REALPATH ..."

echo "running SQL now ..."
docker run -it --rm -v $HOME:/root -v $REALPATH:/tmp/sqlfile.sql broadinstitute/dsde-toolbox:dev mysql-connect.sh -p firecloud -e $ENV -a rawls -f /tmp/sqlfile.sql
echo "SQL complete."

echo "rawls sqlrunner.sh done"