#!/bin/bash

if [ -z "$1" ]
  then
    echo
    echo "Must supply environment, e.g. ./list-invalid-entity-caches.sh dev"
    echo
    exit 1
fi

set -eu

ENV=$1

echo
echo "Now querying the [$ENV] environment for any invalid entity statistics caches. If there is no SQL output, there"
echo "are no invalid caches. If there is SQL output, it will list all workspaces that have an invalid cache. "
echo
echo "To fix the invalid caches, see the delete-invalid-entity-caches.sh script."
echo
echo

./sqlrunner.sh $ENV sql/list-invalid-entity-caches.sql
echo