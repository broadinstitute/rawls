#!/bin/bash

# This script provides an entry point to assemble the Rawls jar file.
# Used by the rawls-build.yaml workflow in terra-github-workflows.
# chmod +x must be set for this script
set -e

# make jar.  cache sbt dependencies. capture output and stop db before returning.
docker run --rm -e DOCKER_TAG -e GIT_COMMIT -e BUILD_NUMBER -v $PWD:/working -v sbt-cache:/root/.sbt -v jar-cache:/root/.ivy2 -v coursier-cache:/root/.cache/coursier sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.13.13 /working/docker/clean_install.sh /working
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    echo "jar build exited with status $EXIT_CODE"
    exit $EXIT_CODE
fi
