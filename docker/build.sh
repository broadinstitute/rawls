#!/bin/bash

HELP_TEXT="$(cat <<EOF
 Build jar and docker images.
   jar: build jar
   publish: push jar to artifactory
   -d | --docker : (default: no action) provide either "build" or "push" to
           build or push a docker image.  "push" will also perform build.
   -g | --gcr-registry: If this flag is set, will push to the specified GCR repository.
   -k | --service-account-key-file: (optional) path to a service account key json
           file. If set, the script will call "gcloud auth activate-service-account".
           Otherwise, the script will not authenticate with gcloud.
   -h | --help: print help text.
 Examples:
   Jenkins build job should run with all options, for example,
     ./docker/build.sh jar publish -d push -g "my-gcr-registry" -k "path-to-my-keyfile"
\t
EOF
)"

# Enable strict evaluation semantics
set -e

echo "rawls docker/build.sh starting ..."

# Set default variables
DOCKER_CMD=
BRANCH=${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}  # default to current branch
GCR_REGISTRY=${GCR_REGISTRY:-gcr.io/broad-dsp-gcr-public/rawls}
ENV=${ENV:-""}
SERVICE_ACCT_KEY_FILE=""

MAKE_JAR=false
PUSH_ARTIFACTORY=false
RUN_DOCKER=false
PRINT_HELP=false

if [ -z "$1" ]; then
    echo "No argument supplied!"
    echo "run '${0} -h' to see available arguments."
    exit 1
fi
while [ "$1" != "" ]; do
    case $1 in
        jar)
            MAKE_JAR=true
            ;;
        publish)
            PUSH_ARTIFACTORY=true
            ;;
        -d | --docker)
            shift
            echo "docker command = $1"
            DOCKER_CMD=$1
            RUN_DOCKER=true
            ;;
        -g | --gcr-registry)
            shift
            echo "gcr registry = $1"
            GCR_REGISTRY=$1
            ;;
        -k | --service-account-key-file)
            shift
            echo "service-account-key-file = $1"
            SERVICE_ACCT_KEY_FILE=$1
            ;;
        -h | --help)
            PRINT_HELP=true
            ;;
        *)
            echo "Urecognized argument '${1}'."
            echo "run '${0} -h' to see available arguments."
            exit 1
            ;;

    esac
    shift
done

if $PRINT_HELP; then
    echo -e "${HELP_TEXT}"
    exit 0
fi

# Run gcloud auth if a service account key file was specified.
if [[ -n $SERVICE_ACCT_KEY_FILE ]]; then
  TMP_DIR=$(mktemp -d tmp-XXXXXX)
  export CLOUDSDK_CONFIG=$(pwd)/${TMP_DIR}
  gcloud auth activate-service-account --key-file="${SERVICE_ACCT_KEY_FILE}"
fi

function make_jar()
{
    echo "building jar..."

    # make jar.  cache sbt dependencies. capture output and stop db before returning.
    DOCKER_RUN="docker run --rm"

    # TODO: DOCKER_TAG hack until JAR build migrates to Dockerfile. Tell SBT to name the JAR
    # `rawls-assembly-local-SNAP.jar` instead of including the commit hash. Otherwise we get
    # an explosion of JARs that all get copied to the image; and which one runs is undefined.
    DOCKER_RUN="$DOCKER_RUN -e DOCKER_TAG=local -e GIT_COMMIT -e BUILD_NUMBER -v $PWD:/working -v sbt-cache:/root/.sbt -v jar-cache:/root/.ivy2 -v coursier-cache:/root/.cache/coursier sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.10.0_2.13.14 /working/docker/clean_install.sh /working"

    JAR_CMD=$($DOCKER_RUN 1>&2)
    EXIT_CODE=$?

    # if jar is a fail, fail script
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
    docker run --rm -e GIT_HASH=$GIT_HASH -v $PWD:/$PROJECT -v sbt-cache:/root/.sbt -v jar-cache:/root/.ivy2 -v coursier-cache:/root/.cache/coursier -w="/$PROJECT" -e ARTIFACTORY_USERNAME=$ARTIFACTORY_USERNAME -e ARTIFACTORY_PASSWORD=$ARTIFACTORY_PASSWORD sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.10.0_2.13.14 /$PROJECT/core/src/bin/publishSnapshot.sh
}

function docker_cmd()
{
    if [ $DOCKER_CMD = "build" ] || [ $DOCKER_CMD = "push" ]; then
        GIT_SHA=$(git rev-parse ${BRANCH})
        echo GIT_SHA=$GIT_SHA > env.properties
        HASH_TAG=${GIT_SHA:0:12}

        echo "building ${GCR_REGISTRY}:${HASH_TAG}..."
        docker build --pull -t ${GCR_REGISTRY}:${HASH_TAG} .

        if [ $DOCKER_CMD = "push" ]; then
            echo "pushing ${GCR_REGISTRY}:${HASH_TAG}..."
            docker push ${GCR_REGISTRY}:${HASH_TAG}
        fi
    else
        echo "Not a valid docker option!  Choose either build or push (which includes build)"
    fi
}

function cleanup()
{
    echo "cleaning up..."
    if [[ -n $SERVICE_ACCT_KEY_FILE ]]; then
      gcloud auth revoke && echo 'Token revoke succeeded' || echo 'Token revoke failed -- skipping'
      rm -rf ${CLOUDSDK_CONFIG}
    fi
}

if $MAKE_JAR; then
    make_jar
fi

if $PUSH_ARTIFACTORY; then
    artifactory_push
fi

if $RUN_DOCKER; then
    docker_cmd
fi

cleanup

echo "rawls docker/build.sh done"
