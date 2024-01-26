# pact4s [Under construction]

pact4s is used for contract testing.

## Run Pact tests locally
On the command line, from the Rawls top-level directory run the following:


```shell
export PACT_BROKER_URL="https://pact-broker.dsp-eng-tools.broadinstitute.org/"
export PACT_BROKER_USERNAME=$(vault read -field=basic_auth_read_only_username secret/dsp/pact-broker/users/read-only)
export PACT_BROKER_PASSWORD=$(vault read -field=basic_auth_read_only_password secret/dsp/pact-broker/users/read-only)
sbt "project pact4s" test
```

