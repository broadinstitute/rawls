#!/bin/bash

# Run tests and make jar
# Uses docker.

set -eux

# check if mysql is running
CONTAINER=mysql
RUNNING=$(docker inspect -f {{.State.Running}} $CONTAINER || echo "false")

# mysql set-up
if ! $RUNNING; then
    # force remove mysql in case it is stopped
    echo "attempting to remove old mysql container..."
    docker rm -f $CONTAINER || echo "docker rm failed. nothing to rm."

    # start up mysql
    echo "starting up mysql container..."
    docker run --name $CONTAINER -e MYSQL_ROOT_PASSWORD=rawls-test -e MYSQL_USER=rawls-test -e MYSQL_PASSWORD=rawls-test -e MYSQL_DATABASE=testdb -d -p 3310:3306 mysql/mysql-server:5.7.15

    # validate mysql
    echo "running mysql validation..."
    docker run --rm --link mysql:mysql -v $PWD/docker/sql_validate.sh:/working/sql_validate.sh broadinstitute/dsde-toolbox /working/sql_validate.sh
    echo "mysql validation succeeded."
fi

# Get the last commit hash of the model directory and set it as an environment variable
GIT_MODEL_HASH=$(git log -n 1 --pretty=format:%h model)

# make jar.  cache sbt dependencies.
docker run --rm --link mysql:mysql -e GIT_MODEL_HASH=$GIT_MODEL_HASH -v $PWD:/working -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 broadinstitute/scala-baseimage /working/docker/install.sh /working

