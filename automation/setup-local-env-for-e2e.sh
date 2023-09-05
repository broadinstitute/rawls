#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(pwd)
TEST_RESOURCES=${SCRIPT_DIR}/src/test/resources

# Required values
e2eEnv=""
bee=""
billingProject=""

# Function to display usage/help message
usage() {
    echo "Usage: $0 --e2eEnv <value> --bee <value> [--billingProject <value>] [--tenantId <value>] [--subscriptionId <value>] [--landingZoneId <value>]"
    echo "  --e2eEnv: The name of the .env file that contains envvars for E2E tests (e.g. The Base64-encoded User Data in JSON format (ex: [{\"email\":\"hermione.owner@quality.firecloud.org\",\"type\":\"owner\",\"bearer\":\"yadayada\"},{\"email\":\"harry.potter@quality.firecloud.org\",\"type\":\"student\",\"bearer\":\"yadayada2\"}]) is stored in USERS_METADATA_B64 envvar."
    echo "  --bee: The name of an existing BEE environment."
    echo "  --billingProject: The name of a valid billing project in the given BEE environment that has already been attached to Azure Landing Zone.."
    echo ""
    echo "# Use Case"
    echo "# ========="
    echo "#"
    echo "# $0 --e2eEnv azure_e2e.env --bee rawls-593017852-1-dev --billingProject tmp-billing-project-44302a2c-5"
    echo "#"
    echo "# Replace rawls-593017852-1-dev with the name of an existing BEE environment you want your tests to run against"
    echo "# Replace tmp-billing-project-44302a2c-5 with the name of a valid billing project in the BEE environment"
    echo "# Please refer to https://github.com/broadinstitute/terra-github-workflows/blob/main/.github/workflows/attach-billing-project-to-landing-zone.yaml"
    echo "#"
    echo "# The above script generates a azure_e2e.env file (or whatever filename you configured for the --e2eEnv argument) under the src/test/resources directory. You can run tests locally as follows."
    echo "#"
    echo "# source ${TEST_RESOURCES}/azure_e2e.env"
    echo "# sbt \"testOnly -- -l ProdTest -l NotebooksCanaryTest -n org.broadinstitute.dsde.test.api.WorkspacesAzureTest\""
    echo "#"
    echo "# If you are running the test via IntelliJ, in the Run/Debug Configuration use the EnvFile tab to provide the path to the generated .env file."
    echo "#"
    exit 1
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --e2eEnv)
            e2eEnv="$2"
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
if [[ -z "$e2eEnv" || -z "$bee" || -z "$billingProject" ]]; then
    echo "Usage: $0 --e2eEnv <value> --bee <value> --billingProject <value> [--tenantId <value>] [--subscriptionId <value>] [--mrgId <value>] [--landingZoneId <value>]"
    echo "Use '$0 --help' to see all available options."
    exit 1
fi

echo "export SCRIPT_DIR=\"${SCRIPT_DIR}\"" > ${TEST_RESOURCES}/$e2eEnv
echo "export E2E_ENV=${e2eEnv}" >> ${TEST_RESOURCES}/$e2eEnv
echo "export BEE_ENV=\"${bee}\"" >> ${TEST_RESOURCES}/$e2eEnv
echo "export BILLING_PROJECT=\"${billingProject}\"" >> ${TEST_RESOURCES}/$e2eEnv

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

echo "export FC_ID=${FC_ID}" >> ${TEST_RESOURCES}/$e2eEnv

QA_EMAIL=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN                      \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_USERS_PATH}                               \
    | jq -r .data.service_acct_email)

echo "export QA_EMAIL=${QA_EMAIL}" >> ${TEST_RESOURCES}/$e2eEnv

TRIAL_BILLING_CLIENT_ID=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN       \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${TRIAL_BILLING_ACCOUNT_PATH}                  \
    | jq -r .data.client_email)

echo "export TRIAL_BILLING_CLIENT_ID=${TRIAL_BILLING_CLIENT_ID}" >> ${TEST_RESOURCES}/$e2eEnv

ORCH_STORAGE_SIGNING_SA=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN       \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${RAWLS_ACCOUNT_PATH}                          \
    | jq -r .data.client_email)

echo "export ORCH_STORAGE_SIGNING_SA=${ORCH_STORAGE_SIGNING_SA}" >> ${TEST_RESOURCES}/$e2eEnv

BILLING_ACCOUNT_ID=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN            \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_SECRETS_PATH}                             \
    | jq -r .data.trial_billing_account)

echo "export BILLING_ACCOUNT_ID=${BILLING_ACCOUNT_ID}" >> ${TEST_RESOURCES}/$e2eEnv

AUTO_USERS_PASSWD=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN             \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_USERS_PATH}                               \
    | jq -r .data.automation_users_passwd)

echo "export AUTO_USERS_PASSWD=${AUTO_USERS_PASSWD}" >> ${TEST_RESOURCES}/$e2eEnv

USERS_PASSWD=$(docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN                  \
    ${DSDE_TOOLBOX_DOCKER_IMAGE}                                            \
    vault read --format=json ${FC_USERS_PATH}                               \
    | jq -r .data.users_passwd)

echo "export USERS_PASSWD=${USERS_PASSWD}" >> ${TEST_RESOURCES}/$e2eEnv

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

    echo "export USERS_METADATA_B64=\"${USERS_METADATA_JSON_B64}\"" >> ${TEST_RESOURCES}/$e2eEnv
}

obtainUserTokens

source ${TEST_RESOURCES}/$e2eEnv

# Read the template file and perform the substitution
template="application.bee.conf.template"
conf="${TEST_RESOURCES}/application.conf"

envsubst < ${template} > ${conf}
