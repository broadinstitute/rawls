#!/usr/bin/env bash

# check if mysql is running
CONTAINER=opendj

start() {

    echo "attempting to remove old opendj container..."
    docker rm -f $CONTAINER || echo "docker rm failed. nothing to rm."

    echo "starting up  container..."
    docker run --name $CONTAINER -e ROOTPASS="testtesttest" -e BASE_DN=dc=dsde-dev,dc=broadinstitute,dc=org -v ${PWD}/docker/opendjsetup.sh:/opt/opendj/bootstrap/setup.sh -v ${PWD}/docker/example-v1.json:/opt/example-v1.json -d -p 3389:389 broadinstitute/openam:opendj
    echo "sleeping 40 seconds til opendj is up and happy. This does not check anything."
    sleep 40

}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER || echo "$CONTAINER stop failed. container already stopped."
    docker rm -v $CONTAINER || echo "$CONTAINER rm -v failed.  container already destroyed."
}

COMMAND=$1
if [ $COMMAND = "start" ]; then
    start
elif [ $COMMAND = "stop" ]; then
    stop
else
    exit 1
fi