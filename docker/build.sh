#!/bin/bash

# Run tests and make jar
# Uses docker.

set -eux

bash ./docker/run-mysql.sh

# Get the last commit hash of the model directory and set it as an environment variable
GIT_MODEL_HASH=$(git log -n 1 --pretty=format:%h model)

# make jar.  cache sbt dependencies.
docker run --rm --link mysql:mysql -e GIT_MODEL_HASH=$GIT_MODEL_HASH -v $PWD:/working -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 broadinstitute/scala-baseimage /working/docker/install.sh /working

