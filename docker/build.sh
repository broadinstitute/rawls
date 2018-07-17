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
    bash ./docker/run-mysql.sh start
    bash ./docker/run-opendj.sh start

    # Get the last commit hash of the model directory and set it as an environment variable
    GIT_MODEL_HASH=$(git log -n 1 --pretty=format:%h model)

    # make jar.  cache sbt dependencies. capture output and stop db before returning.
    JAR_CMD=`docker run --rm --link mysql:mysql --link opendj:opendj -e SKIP_TESTS=$SKIP_TESTS -e GIT_MODEL_HASH=$GIT_MODEL_HASH -v $PWD:/working -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 broadinstitute/scala-baseimage /working/docker/install.sh /working`
    EXIT_CODE=$?

    # stop mysql and opendj
    bash ./docker/run-mysql.sh stop
    bash ./docker/run-opendj.sh stop

    # if tests were a fail, fail script
    if [ $EXIT_CODE != 0 ]; then
        exit $EXIT_CODE
    fi
}

function artifactory_push()
{
    VAULT_TOKEN=${VAULT_TOKEN:-$(cat /etc/vault-token-dsde)}
    ARTIFACTORY_USERNAME=dsdejenkins
    ARTIFACTORY_PASSWORD=$(docker run -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox vault read -field=password secret/dsp/accts/artifactory/dsdejenkins)
    echo "Publishing to artifactory..."
    docker run --rm -v $PWD:/$PROJECT -v jar-cache:/root/.ivy -v jar-cache:/root/.ivy2 -w="/$PROJECT" -e ARTIFACTORY_USERNAME=$ARTIFACTORY_USERNAME -e ARTIFACTORY_PASSWORD=$ARTIFACTORY_PASSWORD broadinstitute/scala-baseimage:scala-2.11.8 /$PROJECT/core/src/bin/publishSnapshot.sh
}

function docker_cmd()
{
    if [ $DOCKER_CMD = "build" ] || [ $DOCKER_CMD = "push" ]; then
        echo "building $PROJECT docker image..."
        echo GIT_SHA=$GIT_SHA > env.properties
        HASH_TAG=${GIT_SHA:0:12}

        echo "building $PROJECT-tests docker image..."
        docker build -t $REPO:${HASH_TAG} .
        cd automation
        docker build -f Dockerfile-tests -t $TESTS_REPO:${HASH_TAG} .
        cd ..

        if [ $DOCKER_CMD = "push" ]; then
            echo "pushing $PROJECT:$HASH_TAG docker image..."
            docker push $REPO:${HASH_TAG}

            echo "pushing $PROJECT:$BRANCH docker image..."
            docker tag $REPO:${HASH_TAG} $REPO:${BRANCH}
            docker push $REPO:${BRANCH}

            echo "pushing $PROJECT-tests:${HASH_TAG} docker image..."
            docker push $TESTS_REPO:${HASH_TAG}

            echo "pushing $PROJECT-tests:$BRANCH docker image..."
            docker tag $TESTS_REPO:${HASH_TAG} $TESTS_REPO:${BRANCH}
            docker push $TESTS_REPO:${BRANCH}
        fi
    else
        echo "Not a valid docker option!  Choose either build or push (which includes build)"
    fi
}

# parse command line options
DOCKER_CMD=
BRANCH=${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}  # default to current branch
REPO=${REPO:-broadinstitute/$PROJECT}
TESTS_REPO=$REPO-tests
ENV=${ENV:-""}  # if env is not set, push an image with branch name
GIT_SHA=$(git rev-parse origin/${BRANCH})


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
