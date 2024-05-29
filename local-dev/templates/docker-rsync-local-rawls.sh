#!/bin/bash
# v1.1.0 rmeffan@broad
# v1.0 zarsky@broad

# Run rawls locally. By default this will launch rawls as a front end instance accessing
# the dev rawls database and dev sam. To run with a local database set the env var LOCAL_MYSQL
# to 'true'. The local database is stored in a docker volume named local-rawls-mysqlstore.
# Run `docker volume rm local-rawls-mysqlstore` to remove it and start fresh.
# Set the env var BACK_RAWLS to 'true' to start a backend instance.
# Set the env var LOCAL_SAM to 'true' if running a local instance of sam.

#
# Functions
#

clean_up () {
    echo
    echo "Cleaning up after myself..."
    docker rm -f rawls-rsync-container rawls-proxy rawls-sbt sqlproxy rawls_mysql
    docker network rm fc-rawls
    pkill -P $$
    [[ -f ./.docker-rsync-local.pid ]] && rm ./.docker-rsync-local.pid
    [[ -f config/built-config.conf ]] && rm config/built-config.conf
}

make_config () {
    echo "include \"rawls.conf\"" > config/built-config.conf

    if [[ "$LOCAL_SAM" == 'true' ]]; then
        echo "Using local sam ..."
        echo "include \"local-sam.conf\"" >> config/built-config.conf
    fi
    if [[ "$LOCAL_MYSQL" == 'true' ]]; then
        echo "Using local mysql ..."
        echo "include \"local-mysql.conf\"" >> config/built-config.conf
    fi
}

run_rsync ()  {
    rsync --blocking-io -azl --delete -e "docker exec -i" . rawls-rsync-container:working \
        --filter='+ /build.sbt' \
        --filter='+ /config/***' \
        --filter='- /core/target/***' \
        --filter='+ /core/***' \
        --filter='- /model/target/***' \
        --filter='+ /model/***' \
        --filter='- /metrics/target/***' \
        --filter='+ /metrics/***' \
        --filter='- /util/target/***' \
        --filter='+ /util/***' \
        --filter='- /google/target/***' \
        --filter='+ /google/***' \
        --filter='- /project/project/target/***' \
        --filter='- /project/target/***' \
        --filter='+ /project/***' \
        --filter='+ /.git/***' \
        --filter='- *'
}

start_server () {
    docker network create fc-rawls

    echo "Creating Google sqlproxy container..."
    source ./config/sqlproxy.env
    docker create --name sqlproxy \
        --restart "always" \
        --network="fc-rawls" \
        gcr.io/cloudsql-docker/gce-proxy:1.30.0 /cloud_sql_proxy -credential_file=/etc/sqlproxy-service-account.json -instances=${GOOGLE_PROJECT}:${CLOUDSQL_ZONE}:${CLOUDSQL_INSTANCE}=tcp:0.0.0.0:3306

    docker cp config/sqlproxy-service-account.json sqlproxy:/etc/sqlproxy-service-account.json

    JAVA_OPTS='-Dconfig.file=/app/config/built-config.conf'

    if [[ "$BACK_RAWLS" == 'true' ]]; then
        echo "Booting rawls as BACK ..."
        JAVA_OPTS+=' -DbackRawls=true'
    else
        echo "Booting rawls as FRONT ..."
        JAVA_OPTS+=' -DbackRawls=false'
    fi

    # Get the last commit hash and set it as an environment variable
    GIT_HASH=$(git log -n 1 --pretty=format:%h)

    echo "Creating SBT docker container..."
    docker create -it --name rawls-sbt \
    -v ~/.m2:/root/.m2 \
    -v rawls-shared-source:/app -w /app \
    -v sbt-cache:/root/.sbt \
    -v jar-cache:/root/.ivy2 \
    -v ~/.ivy2/local:/root/.ivy2/local \
    -v coursier-cache:/root/.cache/coursier \
    -p 25050:5050 \
    --network=fc-rawls \
    -e HOSTNAME=$HOSTNAME \
    -e JAVA_OPTS="$JAVA_OPTS" \
    -e GOOGLE_APPLICATION_CREDENTIALS='/etc/rawls-account.json' \
    -e GIT_HASH=$GIT_HASH \
    sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.13.13 \
    bash -c "git config --global --add safe.directory /app && sbt clean \~reStart"

    docker cp config/rawls-account.pem rawls-sbt:/etc/rawls-account.pem
    docker cp config/rawls-account.json rawls-sbt:/etc/rawls-account.json
    docker cp config/billing-account.pem rawls-sbt:/etc/billing-account.pem
    docker cp config/buffer-account.json rawls-sbt:/etc/buffer-account.json
    docker cp config/bigquery-account.json rawls-sbt:/etc/bigquery-account.json

    if [[ "$LOCAL_MYSQL" == 'true' ]]; then
        echo "Creating mysql..."
	    docker run --network=fc-rawls --name rawls_mysql -e MYSQL_ROOT_HOST='%' -e MYSQL_ROOT_PASSWORD=rawls-test -e MYSQL_USER=rawls-test -e MYSQL_PASSWORD=rawls-test -e MYSQL_DATABASE=rawls -v local-rawls-mysqlstore:/var/lib/mysql -d mysql/mysql-server:5.7 --character-set-server=utf8

        sleep 5
    fi

    echo "Creating proxy..."
    docker create --name rawls-proxy \
    --restart "always" \
    --network=fc-rawls \
    -p 20080:80 -p 20443:443 \
    -e APACHE_HTTPD_TIMEOUT='650' \
    -e APACHE_HTTPD_KEEPALIVE='On' \
    -e APACHE_HTTPD_KEEPALIVETIMEOUT='650' \
    -e APACHE_HTTPD_MAXKEEPALIVEREQUESTS='500' \
    -e APACHE_HTTPD_PROXYTIMEOUT='650' \
    -e PROXY_TIMEOUT='650' \
    -e PROXY_URL='http://rawls-sbt:8080/' \
    -e PROXY_URL2='http://rawls-sbt:8080/api' \
    -e PROXY_URL3='http://rawls-sbt:8080/register' \
    -e CALLBACK_URI='https://local.broadinstitute.org/oauth2callback' \
    -e LOG_LEVEL='debug' \
    -e SERVER_NAME='local.broadinstitute.org' \
    -e APACHE_HTTPD_TIMEOUT='650' \
    -e APACHE_HTTPD_KEEPALIVE='On' \
    -e APACHE_HTTPD_KEEPALIVETIMEOUT='650' \
    -e APACHE_HTTPD_MAXKEEPALIVEREQUESTS='500' \
    -e APACHE_HTTPD_PROXYTIMEOUT='650' \
    -e PROXY_TIMEOUT='650' \
    -e REMOTE_USER_CLAIM='sub' \
    -e ENABLE_STACKDRIVER='yes' \
    -e FILTER2='AddOutputFilterByType DEFLATE application/json text/plain text/html application/javascript application/x-javascript' \
    us.gcr.io/broad-dsp-gcr-public/httpd-terra-proxy:v0.1.16

    docker cp config/server.crt rawls-proxy:/etc/ssl/certs/server.crt
    docker cp config/server.key rawls-proxy:/etc/ssl/private/server.key
    docker cp config/ca-bundle.crt rawls-proxy:/etc/ssl/certs/server-ca-bundle.crt
    docker cp config/oauth2.conf rawls-proxy:/etc/httpd/conf.d/oauth2.conf
    docker cp config/site.conf rawls-proxy:/etc/httpd/conf.d/site.conf

    echo "Starting sqlproxy..."
    docker start sqlproxy
    echo "Starting proxy..."
    docker start rawls-proxy
    echo "Starting SBT..."
    docker start -ai rawls-sbt
}

#
# Main
#

# Step 0 - Prereq Check
hash fswatch 2>/dev/null || {
    echo >&2 "This script requires fswatch (https://github.com/emcrisostomo/fswatch), but it's not installed. On Darwin, just \"brew install fswatch\".  Aborting."; exit 1;
}

# Step 1 - Clean up remains of previous incomplete runs
if [ -a ./.docker-rsync-local.pid ]; then
    echo "Looks like clean-up wasn't completed, doing it now..."
    docker rm -f rawls-rsync-container rawls-proxy rawls-sbt
    docker network rm fc-rawls
    pkill -P $(< "./.docker-rsync-local.pid")
    rm ./.docker-rsync-local.pid
    rm config/built-config.conf
fi

# Step 2 - Initialization

#Configure a trap to enable proper cleanup if script exits prematurely.
trap clean_up EXIT HUP INT QUIT PIPE TERM 0 20

echo "Creating shared volumes if they don't exist..."
docker volume create --name rawls-shared-source
docker volume create --name sbt-cache
docker volume create --name jar-cache
docker volume create --name coursier-cache

# Step 3 - Generate configuration and start rsync container
echo "Building config..."
make_config

echo "Launching rsync container..."
docker run -d \
    --name rawls-rsync-container \
    -v rawls-shared-source:/working \
    -e DAEMON=docker \
    tjamet/rsync

# Step 4 - Perform the rsync
echo "Performing initial file sync..."
run_rsync
fswatch -o . | while read f; do run_rsync; done &
echo $$ > ./.docker-rsync-local.pid

# Step 5 - Start
start_server
