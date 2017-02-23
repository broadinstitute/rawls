#!/bin/bash

# Run tests and make jar

set -eux

# check if mysql is running
mysql_running=$(docker inspect -f {{.State.Running}} mysql)
if ! $mysql_running ; then
    # start up mysql
    docker pull mysql/mysql-server:5.7.15
    docker run --name mysql -e MYSQL_ROOT_PASSWORD=rawls-test -e MYSQL_USER=rawls-test -e MYSQL_PASSWORD=rawls-test -e MYSQL_DATABASE=testdb -d -p 3310:3306 mysql/mysql-server:5.7.15

    # validate mysql
    sudo yum -y install mysql
    sleep 5
    mysql -uroot -prawls-test --host=127.0.0.1 --port=3310 -e "SELECT VERSION();SELECT NOW()"
    mysql -urawls-test -prawls-test --host=127.0.0.1 --port=3310 -e "SELECT VERSION();SELECT NOW()"
    mysql -urawls-test -prawls-test --host=127.0.0.1 --port=3310 -e "SELECT VERSION();SELECT NOW()" testdb
fi

# Get the last commit hash of the model directory and set it as an environment variable
GIT_MODEL_HASH=$(git log -n 1 --pretty=format:%h model)

# make jar.  cache sbt dependencies.
docker run --rm --link mysql:mysql -e GIT_MODEL_HASH=$GIT_MODEL_HASH -v $PWD:/working -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 broadinstitute/scala-baseimage /working/docker/install.sh /working

