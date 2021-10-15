#!/bin/bash

if [ -z "$1" ]
  then
    echo
    echo "Must supply environment, e.g. ./delete-invalid-entity-caches.sh dev"
    echo
    exit 1
fi

set -eu

ENV=$1

echo
echo "This will write to the [$ENV] environment and delete all invalid entity statistics caches. Rawls will rebuild"
echo "these caches at some point after they have been deleted."
echo
echo "To see which workspaces have an invalid cache, see the list-invalid-entity-caches.sh script."
echo
echo "THIS DELETION CANNOT BE UNDONE, even though it should be safe."
echo
read -p "Enter 'y' to continue, anything else to quit: " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  echo "aborting script."
  exit 1
fi

echo
./sqlrunner.sh $ENV sql/delete-invalid-entity-caches.sql
echo