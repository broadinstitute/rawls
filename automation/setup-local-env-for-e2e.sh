#!/usr/bin/env bash

# Use this script to set up your local environment that can be used for running Azure E2E tests against an existing BEE
# Run from automation/

# Use --help option to see all available options
#
# Use Cases
# =========
# $0 is the name of this script
#
# Use Case 1 (Create a random billing project and attach it to default Landing Zone):
#
# $0 --usersEnv USERS_METADATA_B64 --bee rawls-593017852-dev-1
#
# Replace rawls-593017852-dev-1 with the name of an existing BEE environment you want your tests to run against
#
# Use Case 2 (Reuse a billing project that already exists in the BEE environment):
#
# $0 --usersEnv USERS_METADATA_B64 --bee rawls-593017852-dev-1 --billingProject tmp-billing-project-44302a2c-5
#
# Replace rawls-593017852-dev-1 with the name of an existing BEE environment you want your tests to run against
# Replace tmp-billing-project-44302a2c-5 with the name of a valid billing project in the BEE environment
#
# Use Case 3 (Create a random billing project and attach it to new Landing Zone):
#
# $0 --usersEnv USERS_METADATA_B64 --bee rawls-593017852-dev-1 \
#    --tenantId fad90753-2022-4456-9b0a-c7e5b934e408           \
#    --subscriptionId f557c728-871d-408c-a28b-eb6b2141a087     \
#    --mrgId staticTestingMrg                                  \
#    --landingZoneId f41c1a97-179b-4a18-9615-5214d79ba600
#
# Replace rawls-593017852-dev-1 with the name of an existing BEE environment you want your tests to run against
# Replace (tenantId, subscriptionId, mrgId, landingZoneId) with new coordinates
#
# This script generates a e2e.env file. You can run tests locally as follows.
#
# source e2e.env
# sbt "testOnly -- -l ProdTest -l NotebooksCanaryTest -n org.broadinstitute.dsde.test.api.WorkspacesAzureTest"
#

set -e

SCRIPT_DIR=$(pwd)

echo "export SCRIPT_DIR=\"${SCRIPT_DIR}\"" > e2e.env

# Required values
usersEnv=""
bee=""
# Optional values
billingProject=""
tenantId="fad90753-2022-4456-9b0a-c7e5b934e408"
subscriptionId="f557c728-871d-408c-a28b-eb6b2141a087"
mrgId="staticTestingMrg"
landingZoneId="f41c1a97-179b-4a18-9615-5214d79ba600"

# Function to display usage/help message
usage() {
    echo "Usage: $0 --usersEnv <value> --bee <value> [--billingProject <value>] [--tenantId <value>] [--subscriptionId <value>] [--landingZoneId <value>]"
    echo "  --usersEnv: The name of the environment variable that stores the Base64-encoded User Data in JSON format (ex: [{\"email\":\"hermione.owner@quality.firecloud.org\",\"type\":\"owner\",\"bearer\":\"yadayada\"},{\"email\":\"harry.potter@quality.firecloud.org\",\"type\":\"student\",\"bearer\":\"yadayada2\"}])."
    echo "  --bee: The name of an existing BEE environment."
    echo "  --billingProject: The name of an existing billing project in the given BEE environment. If not specified, a random billing project will be created and attached to the landing zone (optional: the attachLandingZoneToBillingProject method will be skipped, the landing zone must already been attached to the specified billing project)."
    echo "  --tenantId: Azure tenant ID (optional, default: fad90753-2022-4456-9b0a-c7e5b934e408)."
    echo "  --subscriptionId: Azure subscription ID (optional, default: f557c728-871d-408c-a28b-eb6b2141a087)."
    echo "  --mrgId: Azure Managed Resource Group name (optional, default: staticTestingMrg)."
    echo "  --landingZoneId: Landing Zone ID. An existing LZID tag within a given MRG (optional, default: f41c1a97-179b-4a18-9615-5214d79ba600)."
    echo ""
    echo "# Use Cases"
    echo "# ========="
    echo "#"
    echo "# Use Case 1 (Create a random billing project and attach it to default Landing Zone):"
    echo "#"
    echo "# $0 --usersEnv USERS_METADATA_B64 --bee rawls-593017852-1-dev"
    echo "#"
    echo "# Replace rawls-593017852-1-dev with the name of an existing BEE environment you want your tests to run against"
    echo "#"
    echo "# Use Case 2 (Reuse a billing project that already exists in the BEE environment):"
    echo "#"
    echo "# $0 --usersEnv USERS_METADATA_B64 --bee rawls-593017852-1-dev --billingProject tmp-billing-project-44302a2c-5"
    echo "#"
    echo "# Replace rawls-593017852-1-dev with the name of an existing BEE environment you want your tests to run against"
    echo "# Replace tmp-billing-project-44302a2c-5 with the name of a valid billing project in the BEE environment"
    echo "#"
    echo "# Use Case 3 (Create a random billing project and attach it to new Landing Zone):"
    echo "#"
    echo "# $0 --usersEnv USERS_METADATA_B64 --bee rawls-593017852-1-dev \\"
    echo "#    --tenantId fad90753-2022-4456-9b0a-c7e5b934e408           \\"
    echo "#    --subscriptionId f557c728-871d-408c-a28b-eb6b2141a087     \\"
    echo "#    --mrgId staticTestingMrg                                  \\"
    echo "#    --landingZoneId f41c1a97-179b-4a18-9615-5214d79ba600"
    echo "#"
    echo "# Replace rawls-593017852-1-dev with the name of an existing BEE environment you want your tests to run against"
    echo "# Replace (tenantId, subscriptionId, mrgId, landingZoneId) with new coordinates"
    echo "#"
    echo "# This script generates a e2e.env file. You can run tests locally as follows."
    echo "#"
    echo "# source e2e.env"
    echo "# sbt \"testOnly -- -l ProdTest -l NotebooksCanaryTest -n org.broadinstitute.dsde.test.api.WorkspacesAzureTest\""
    echo "#"
    exit 1
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --usersEnv)
            usersEnv="$2"
            shift 2
            ;;
        --bee)
            bee="$2"
            shift 2
            ;;
        --billingProject)
            billingProject="$2"
            shift 2
            ;;
        --tenantId)
            tenantId="$2"
            shift 2
            ;;
        --subscriptionId)
            subscriptionId="$2"
            shift 2
            ;;
        --mrgId)
            mrgId="$2"
            shift 2
            ;;
        --landingZoneId)
            landingZoneId="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check if required arguments are provided
if [ -z "$usersEnv" ] || [ -z "$bee" ]; then
    echo "Usage: $0 --usersEnv <value> --bee <value> [--tenantId <value>] [--subscriptionId <value>] [--mrgId <value>] [--landingZoneId <value>]"
    echo "Use '$0 --help' to see all available options."
    exit 1
fi

TEST_RESOURCES="${SCRIPT_DIR}/src/test/resources"

echo "export PIPELINE_ENV=${usersEnv}" >> e2e.env
echo "export BEE_ENV=\"${bee}\"" >> e2e.env

VAULT_TOKEN=$(cat $HOME/.vault-token)
DSDE_TOOLBOX_DOCKER_IMAGE=broadinstitute/dsde-toolbox:latest
FC_ACCOUNT_PATH=secret/dsde/firecloud/qa/common/firecloud-account.json
TRIAL_BILLING_ACCOUNT_PATH=secret/dsde/firecloud/qa/common/trial-billing-account.json
FC_SECRETS_PATH=secret/dsde/firecloud/qa/common/secrets
FC_USERS_PATH=secret/dsde/firecloud/qa/common/users
RAWLS_ACCOUNT_PATH=secret/dsde/firecloud/qa/rawls/rawls-account.json

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN ${DSDE_TOOLBOX_DOCKER_IMAGE}     \
    vault read --format=json ${FC_ACCOUNT_PATH}                              \
    | jq -r .data > ${TEST_RESOURCES}/firecloud-account.json

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN ${DSDE_TOOLBOX_DOCKER_IMAGE}     \
    vault read --format=json ${FC_ACCOUNT_PATH}                              \
    | jq -r .data.private_key > ${TEST_RESOURCES}/firecloud-account.pem

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN ${DSDE_TOOLBOX_DOCKER_IMAGE}     \
    vault read --format=json ${TRIAL_BILLING_ACCOUNT_PATH}                   \
    | jq -r .data.private_key > ${TEST_RESOURCES}/trial-billing-account.pem

cat << EOF > ${TEST_RESOURCES}/users.json
{
  "admins": {
    "dumbledore": "dumbledore.admin@quality.firecloud.org",
    "voldemort": "voldemort.admin@quality.firecloud.org"
  },
  "owners": {
    "hermione": "hermione.owner@quality.firecloud.org",
    "sirius": "sirius.owner@quality.firecloud.org",
    "tonks": "tonks.owner@quality.firecloud.org"
  },
  "curators": {
    "mcgonagall": "mcgonagall.curator@quality.firecloud.org",
    "snape": "snape.curator@quality.firecloud.org",
    "hagrid": "hagrid.curator@quality.firecloud.org",
    "lupin": "lupin.curator@quality.firecloud.org",
    "flitwick": "flitwick.curator@quality.firecloud.org"
  },
  "authdomains": {
    "fred": "fred.authdomain@quality.firecloud.org",
    "george": "george.authdomain@quality.firecloud.org",
    "bill": "bill.authdomain@quality.firecloud.org",
    "percy": "percy.authdomain@quality.firecloud.org",
    "molly": "molly.authdomain@quality.firecloud.org",
    "arthur": "arthur.authdomain@quality.firecloud.org"
  },
  "students": {
    "harry": "harry.potter@quality.firecloud.org",
    "ron": "ron.weasley@quality.firecloud.org",
    "lavender": "lavender.brown@quality.firecloud.org",
    "cho": "cho.chang@quality.firecloud.org",
    "oliver": "oliver.wood@quality.firecloud.org",
    "cedric": "cedric.diggory@quality.firecloud.org",
    "crabbe": "vincent.crabbe@quality.firecloud.org",
    "goyle": "gregory.goyle@quality.firecloud.org",
    "dean": "dean.thomas@quality.firecloud.org",
    "ginny": "ginny.weasley@quality.firecloud.org"
  },
  "temps": {
    "luna": "luna.temp@quality.firecloud.org",
    "neville": "neville.temp@quality.firecloud.org"
  },
  "notebookswhitelisted": {
    "hermione": "hermione.owner@quality.firecloud.org",
    "ron": "ron.weasley@quality.firecloud.org"
  },
  "campaignManagers": {
    "dumbledore": "dumbledore.admin@quality.firecloud.org",
    "voldemort": "voldemort.admin@quality.firecloud.org"
  }
}
EOF

FC_ID=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN                         \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_SECRETS_PATH}                             \
    | jq -r .data.firecloud_id)

echo "export FC_ID=${FC_ID}" >> e2e.env

QA_EMAIL=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN                      \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_USERS_PATH}                               \
    | jq -r .data.service_acct_email)

echo "export QA_EMAIL=${QA_EMAIL}" >> e2e.env

TRIAL_BILLING_CLIENT_ID=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN       \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${TRIAL_BILLING_ACCOUNT_PATH}                  \
    | jq -r .data.client_email)

echo "export TRIAL_BILLING_CLIENT_ID=${TRIAL_BILLING_CLIENT_ID}" >> e2e.env

ORCH_STORAGE_SIGNING_SA=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN       \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${RAWLS_ACCOUNT_PATH}                          \
    | jq -r .data.client_email)

echo "export ORCH_STORAGE_SIGNING_SA=${ORCH_STORAGE_SIGNING_SA}" >> e2e.env

BILLING_ACCOUNT_ID=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN            \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_SECRETS_PATH}                             \
    | jq -r .data.trial_billing_account)

echo "export BILLING_ACCOUNT_ID=${BILLING_ACCOUNT_ID}" >> e2e.env

AUTO_USERS_PASSWD=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN             \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_USERS_PATH}                                    \
    | jq -r .data.automation_users_passwd)

echo "export AUTO_USERS_PASSWD=${AUTO_USERS_PASSWD}" >> e2e.env

USERS_PASSWD=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN                  \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_USERS_PATH}                                    \
    | jq -r .data.users_passwd)

echo "export USERS_PASSWD=${USERS_PASSWD}" >> e2e.env

# Function to obtain user tokens
obtainUserTokens() {
    echo "Obtaining user tokens for hermione, harry, and ron..."
    hermione=$(docker run --rm -v ${SCRIPT_DIR}/src:/src             \
            -i gcr.io/oauth2l/oauth2l fetch                          \
            --credentials /src/test/resources/firecloud-account.json \
            --scope profile,email,openid                             \
            --email hermione.owner@quality.firecloud.org)

    harry=$(docker run --rm -v ${SCRIPT_DIR}/src:/src                \
            -i gcr.io/oauth2l/oauth2l fetch                          \
            --credentials /src/test/resources/firecloud-account.json \
            --scope profile,email,openid                             \
            --email harry.potter@quality.firecloud.org)

    ron=$(docker run --rm -v ${SCRIPT_DIR}/src:/src                  \
            -i gcr.io/oauth2l/oauth2l fetch                          \
            --credentials /src/test/resources/firecloud-account.json \
            --scope profile,email,openid                             \
            --email ron.weasley@quality.firecloud.org)

    USERS_METADATA_JSON="[
      {
        \"email\":\"hermione.owner@quality.firecloud.org\",
        \"type\":\"owner\",
        \"bearer\":\"${hermione}\"
      },
      {
        \"email\":\"harry.potter@quality.firecloud.org\",
        \"type\":\"student\",
        \"bearer\":\"${harry}\"
      },
      {
        \"email\":\"ron.weasly@quality.firecloud.org\",
        \"type\":\"student\",
        \"bearer\":\"${ron}\"
      }
    ]"

    # Try the -w0 option (without line breaks) first
    if base64 --help | grep -q '\-w'; then
      USERS_METADATA_JSON_B64=$(printf '%s' "${USERS_METADATA_JSON}" | base64 -w0)
    # If -w0 is not available, try the -b0 option (also without line breaks)
    elif base64 --help | grep -q '\-b'; then
      USERS_METADATA_JSON_B64=$(printf '%s' "${USERS_METADATA_JSON}" | base64 -b 0)
    else
      echo "Error: No suitable base64 encoding option found."
      exit 1
    fi

    echo "export $usersEnv=\"${USERS_METADATA_JSON_B64}\"" >> e2e.env
}

attachLandingZoneToBillingProject() {
    billingProject=$(echo "tmp-billing-project-$(uuidgen)" | cut -c -30)
    echo "Creating a billing project $billingProject and attaching landing zone (Tenant ID=$tenantId, Subscription ID=$subscriptionId, MRG=$mrgId, Landing Zone ID=$landingZoneId)"
    baseOrchUrl="https://firecloudorch.$bee.bee.envs-terra.bio"

    apiResponse=$(curl -s -w "%{http_code}"                                   \
      -X POST "${baseOrchUrl}/api/billing/v2"                                 \
      -H 'Accept: */*'                                                        \
      -H 'Content-Type: application/json'                                     \
      -H "Authorization: Bearer $hermione"                                    \
      -d '{
        "projectName": "'"${billingProject}"'",
        "managedAppCoordinates": {
          "tenantId": "'"${tenantId}"'",
          "subscriptionId": "'"${subscriptionId}"'",
          "managedResourceGroupId": "'"${mrgId}"'",
          "landingZoneId": "'"${landingZoneId}"'"
        }
      }')
    httpStatus=$(echo "$apiResponse" | tail -c 4)
    jsonData=$(echo "$apiResponse")
    jsonData=${jsonData%$httpStatus}
    if [[ "$httpStatus" == "201" ]]; then
      echo "API response: 201 Created The request has been fulfilled and resulted in Azure Billing Project ${billingProject} being created."
    else
      echo "API response: $http_status Failed to create the Azure Billing Project ${billingProject}."
      echo "$jsonData"
      exit 1
    fi

    billingProjectStatus=""
    counter=0
    while [ "$billingProjectStatus" != "Ready" ] && [ $counter -lt 15 ]; do
      echo "Checking creation status of Azure Billing Project ${billingProject}."
      apiResponse=$(curl -s -w "%{http_code}"                                 \
        -X GET "${baseOrchUrl}/api/billing/v2/${billingProject}"              \
        -H 'Accept: application/json'                                         \
        -H "Authorization: Bearer $hermione")
      httpStatus=$(echo "$apiResponse" | tail -c 4)
      jsonData=$(echo "$apiResponse")
      jsonData=${jsonData%$httpStatus}
      echo "jsonData=$jsonData"
      if [[ "$httpStatus" == "200" ]]; then
        echo "API response: 200 OK"
        echo "$jsonData"
        billingProjectStatus=$(echo "$jsonData" | jq -r '.status')
      else
        echo "API response: $httpStatus Failed to obtain the Azure Billing Project ${billingProject} status."
        exit 1
      fi
      sleep 1
      counter=$((counter + 1))
    done

    if [[ "$billingProjectStatus" == "Ready" ]]; then
      echo "Azure Billing Project ${billingProject} is in Ready status."
    else
      echo "Azure Billing Project ${billingProject} not Ready after ${counter} tries."
      echo "Deleting Azure Billing Project ${billingProject} ..."
      apiResponse=$(curl -s -w "%{http_code}"                                 \
        -X DELETE "${baseOrchUrl}/api/billing/v2/${billingProject}"           \
        -H 'Accept: */*'                                                      \
        -H "Authorization: Bearer $hermione")
      httpStatus=$(echo "$apiResponse" | tail -c 4)
      jsonData=$(echo "$apiResponse")
      jsonData=${jsonData%$httpStatus}
      echo "jsonData=$jsonData"
      if [[ "$httpStatus" == "204" ]]; then
        echo "API response: 204 Azure Billing Project ${billingProject} has been deleted."
      else
        echo "API response: $httpStatus"
        echo "$jsonData"
      fi
    fi

    echo "export BILLING_PROJECT=\"${billingProject}\"" >> e2e.env
}

obtainUserTokens

if [ -z "$billingProject" ]; then
  attachLandingZoneToBillingProject
else
  echo "export BILLING_PROJECT=\"${billingProject}\"" >> e2e.env
fi

source e2e.env

# Read the template file and perform the substitution
template="application.bee.conf.template"
conf="${TEST_RESOURCES}/application.conf"

envsubst < ${template} > ${conf}
