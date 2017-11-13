#!/bin/bash

# Single source of truth for building Rawls.
# @ Jackie Roberti
#
# Provide command line options to do one or several things:
#   jar : build rawls jar
#   publish : run rawls/core/src/bin/publishSnapshot.sh to publish to Artifactory
#   -d | --docker : provide arg either "build" or "push", to build and push docker image
# Jenkins build job should run with all options, for example,
#   ./docker/build.sh jar publish -d push

set -ex
PROJECT=rawls

function make_jar()
{
    echo "building jar..."
#    bash ./docker/run-mysql.sh start
#    bash ./docker/run-opendj.sh start

    # Get the last commit hash of the model directory and set it as an environment variable
    GIT_MODEL_HASH=$(git log -n 1 --pretty=format:%h model)

    # make jar.  cache sbt dependencies. capture output and stop db before returning.
    JAR_CMD=`docker run --rm -e GIT_MODEL_HASH=$GIT_MODEL_HASH -v $PWD:/working -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 broadinstitute/scala-baseimage /working/docker/install.sh /working`
    EXIT_CODE=$?

    # stop mysql and opendj
#    bash ./docker/run-mysql.sh stop
#    bash ./docker/run-opendj.sh stop

    # if tests were a fail, fail script
#    if [ $EXIT_CODE != 0 ]; then
#        exit $EXIT_CODE
#    fi
}

function artifactory_push()
{
    echo "Publishing to artifactory..."
    docker run --rm -v $PWD:/$PROJECT -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 -w="/$PROJECT" -e ARTIFACTORY_USERNAME=$ARTIFACTORY_USERNAME -e ARTIFACTORY_PASSWORD=$ARTIFACTORY_PASSWORD broadinstitute/scala-baseimage:scala-2.11.8 /$PROJECT/core/src/bin/publishSnapshot.sh
}

function docker_cmd()
{
    if [ $DOCKER_CMD = "build" ] || [ $DOCKER_CMD = "push" ]; then
        echo "building docker image..."
        GIT_SHA=$(git rev-parse ${GIT_BRANCH})
        echo GIT_SHA=$GIT_SHA > env.properties  # for jenkins jobs
        docker build -t $REPO:${GIT_SHA:0:12} .

        if [ $DOCKER_CMD = "push" ]; then
            echo "pushing docker image..."
            docker push $REPO:${GIT_SHA:0:12}
        fi
    else
        echo "Not a valid docker option!  Choose either build or push (which includes build)"
    fi
}

# parse command line options
DOCKER_CMD=
GIT_BRANCH=${GIT_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}  # default to current branch
REPO=${REPO:-broadinstitute/$PROJECT}  # default to rawls docker repo
while [ "$1" != "" ]; do
    case $1 in
        jar) make_jar ;;
        publish) artifactory_push ;;
        -d | --docker) shift
                       echo $1
                       DOCKER_CMD=$1
                       docker_cmd
                       ;;
    esac
    shift
done