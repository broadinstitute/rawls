#!/usr/bin/env bash

# The docker version installed on DSP Jenkins does not like the "mysql" docker image, so we use "mysql/mysql-server"
#     instead.
#     TODO: Move to mysql:8.0.35 once we no longer depend on Jenkins.
MYSQL_IMAGE=mysql/mysql-server
MYSQL_VERSION=8.0.32
start() {


    echo "attempting to remove old $CONTAINER container..."
    docker rm -f $CONTAINER || echo "docker rm failed. nothing to rm."

    # start up mysql
    echo "starting up mysql container..."
    # flags for this mysql server:
    #   binlog-format=ROW matches CloudSQL: https://cloud.google.com/sql/docs/mysql/flags#system_flags_changed_in
    #   log_bin_trust_function_creators=ON enables create function and create trigger without the SUPER privilege
    docker run --name $CONTAINER \
                  -e MYSQL_ROOT_HOST='%' -e MYSQL_ROOT_PASSWORD=rawls-test -e MYSQL_USER=rawls-test \
                  -e MYSQL_PASSWORD=rawls-test -e MYSQL_DATABASE=testdb \
                  -d -p 3310:3306 $MYSQL_IMAGE:$MYSQL_VERSION \
                  --character-set-server=utf8 \
                  --collation_server=utf8_general_ci \
                  --sql_mode=STRICT_ALL_TABLES \
                  --log_bin_trust_function_creators=ON

    # validate mysql
    echo "running mysql validation..."
    docker run --rm --link $CONTAINER:mysql \
                  -v $PWD/docker/sql_validate.sh:/working/sql_validate.sh \
                  broadinstitute/dsde-toolbox:mysql8 \
                  /working/sql_validate.sh rawls
    if [ 0 -eq $? ]; then
        echo "mysql validation succeeded."
    else
        echo "mysql validation failed."
        exit 1
    fi

}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER || echo "mysql stop failed. container already stopped."
    docker rm -v $CONTAINER || echo "mysql rm -v failed.  container already destroyed."
}

CONTAINER=mysql
COMMAND=$1

if [ ${#@} == 0 ]; then
    echo "Usage: $0 stop|start"
    exit 1
fi

if [ $COMMAND = "start" ]; then
    start
elif [ $COMMAND = "stop" ]; then
    stop
else
    exit 1
fi
