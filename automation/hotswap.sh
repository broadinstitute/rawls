#!/usr/bin/env bash

# Updates the Rawls jar of a FIAB to reflect current local code.
# Run using "./automation/hotswap.sh <your FIAB name>" at the root of the rawls repo clone

set -eu

# see https://stackoverflow.com/questions/3601515/how-to-check-if-a-variable-is-set-in-bash
if [ -z ${1+x} ]; then
      echo "No arguments supplied. Please provide FIAB name as an argument."
      exit 1
fi

FIAB=$1
ENV=${2:-dev}

printf "Generating the Rawls jar...\n\n"
sbt -Dsbt.log.noformat=true clean assembly
RAWLS_JAR_PATH=$(ls target/scala-2.13/rawls-assembly*)

printf "\n\nJar successfully generated."

RAWLS_JAR_NAME=$(basename $RAWLS_JAR_PATH)

printf "\n\nCopying ${RAWLS_JAR_PATH} to /tmp on FIAB '${FIAB}'...\n\n"
gcloud compute scp ${RAWLS_JAR_PATH} ${FIAB}:/tmp --zone=us-central1-a --project broad-dsde-${ENV}

printf "\n\nRemoving the old Rawls jars on the FIAB...\n\n"
# For some reason including the rm command to be executed on the FIAB within the EOSSH block below doesn't work
gcloud compute ssh --project broad-dsde-${ENV} --zone us-central1-a ${FIAB} -- 'sudo docker exec -it firecloud_rawls-app_1 sh -c "rm -f /rawls/*jar"'

printf "\n\nCopying the Rawls jar to the right location on the FIAB, and restarting the Rawls app and proxy...\n\n"
gcloud compute ssh --project broad-dsde-${ENV} --zone us-central1-a ${FIAB} << EOSSH
    sudo docker cp /tmp/${RAWLS_JAR_NAME} firecloud_rawls-app_1:/rawls/${RAWLS_JAR_NAME}
    sudo docker restart firecloud_rawls-app_1
    sudo docker restart firecloud_rawls-proxy_1
EOSSH
