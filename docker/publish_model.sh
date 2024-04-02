#!/bin/bash

# This script provides an entry point to publish the rawls-model library to Artifactory.
# Used by the rawls-build.yaml workflow in terra-github-workflows.
# chmod +x must be set for this script
set -e

docker run --rm \
  -v $PWD:/rawls \
  -v sbt-cache:/root/.sbt \
  -v jar-cache:/root/.ivy2 \
  -v coursier-cache:/root/.cache/coursier \
  -w="/$PROJECT" \
  -e DOCKER_TAG \
  -e ARTIFACTORY_USERNAME \
  -e ARTIFACTORY_PASSWORD \
  sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.13.13 /rawls/core/src/bin/publishSnapshot.sh