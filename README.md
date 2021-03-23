

[![Build Status](https://github.com/broadinstitute/rawls/workflows/Scala%20tests%20with%20coverage/badge.svg?branch=develop
)](https://travis-ci.com/broadinstitute/rawls?branch=develop)
[![Coverage Status](https://img.shields.io/codecov/c/gh/broadinstitute/rawls)](https://codecov.io/gh/broadinstitute/rawls)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-green)](https://github.com/broadinstitute/rawls/blob/master/LICENSE.txt)

# rawls

1. The workspace/entity/submission/method config manager for Terra
2. Bill Rawls, Deputy Commissioner for Operations from *The Wire*:

![](http://vignette2.wikia.nocookie.net/thewire/images/b/b5/Rawls.jpg)

## Getting started

```sh
git clone git@github.com:broadinstitute/rawls.git
brew install git-secrets # if not already installed
cd rawls
sbt antlr4:antlr4Generate # Generates source code for IntellIJ IDEA
./minnie-kenny.sh -f
sbt clean compile test
```

## Running Locally

### Requirements:

* [Docker Desktop](https://www.docker.com/products/docker-desktop) (4GB+, 8GB recommended)
* Broad internal internet connection (or VPN, non-split recommended)
* Render the local configuration files. From the root of the [firecloud-develop](https://github.com/broadinstitute/firecloud-develop) repo, run:
```sh
./firecloud-setup.sh
```
Note: this script will offer to set up configuration for several other services as well. You can skip those if you only want to set up configuration for Rawls

*  The `/etc/hosts` file on your machine must contain this entry (for calling Rawls endpoints):
```sh
127.0.0.1	local.broadinstitute.org
```

### Running:

After satisfying the above requirements, execute the following command from the root of the Rawls repo:

```sh
./config/docker-rsync-local-rawls.sh
```

If Rawls successfully starts up, you can now access the Rawls Swagger page: https://local.broadinstitute.org:20443/

### Useful Tricks:

#### Front & Back Rawls

By default, a locally run Rawls will boot as a "front" instance of Rawls. A front Rawls will serve all HTTP requests and can modify the database, but it will not do monitoring tasks such as submission monitoring, PFB imports, or Google billing project creation.

If you are developing a ticket that deals with any sort of monitoring or asynchronous features, you will likely want to boot your Rawls as a "back" instance, which will run a fully-featured instance of Rawls with monitoring tasks enabled. To boot your local instance as a "back" instance, run*:

```
BACK_RAWLS=true ./config/docker-rsync-local-rawls.sh
```

***Important**: It is highly recommended that use your own Cloud SQL instance when running an instance of back Rawls. See note below on database work.

#### Developing Database Schema Changes

If you are writing Liquibase migrations or doing database work, it is mandatory that you create your own Cloud SQL instance and develop against that. To point to your own CloudSQL instance, modify `/config/sqlproxy.env` accordingly



## Developer quick links:
* Swagger UI: https://rawls.dsde-dev.broadinstitute.org
* Jenkins: https://dsde-jenkins.broadinstitute.org/job/rawls-dev-build
* Running locally in docker https://github.com/broadinstitute/firecloud-develop

## Unit Testing with MySQL in Docker
Ensure that docker is up to date and initialized.
Spin up mysql locally and validate that it is working:

```sh
./docker/run-mysql.sh start
```

Run tests.

```sh
export SBT_OPTS="-Xmx2G -Xms1G -Dmysql.host=localhost -Dmysql.port=3310"
sbt clean compile test
```

And when you're done, spin down mysql (it is also fine to leave it running for your next round of tests):

```sh
./docker/run-mysql.sh stop
```

## Build Rawls docker image

Note: this may use more than 4.5 GB of Docker memory. If your Docker is not configured with enough memory (Docker for Mac defaults to 2GB), you may see a cryptic error messages saying `Killed`.
 
Build Rawls jar

```sh
./docker/build.sh jar
```

Build Rawls jar and docker image

```sh
./docker/build.sh jar -d build
```

## Publish rawls-model

Supported Scala versions: 2.12, 2.13

Running the `publishRelease.sh` script publishes a release of rawls-model, workbench-util and workbench-google to Artifactory. You should do this manually from the base directory of the repo when you change something in `model/src`, `util/src` or `google/src`.
- [Jenkins runs `publishSnapshot.sh` on every dev build](https://fc-jenkins.dsp-techops.broadinstitute.org/job/rawls-build/), but that makes "unofficial" `-SNAP` versions.
- Note that you need `ARTIFACTORY_USERNAME` and `ARTIFACTORY_PASSWORD` in your env for either of these to work.

To publish an official release, you can run the following command:

```sh
VAULT_TOKEN=$(cat ~/.vault-token) ARTIFACTORY_USERNAME=dsdejenkins ARTIFACTORY_PASSWORD=$(docker run -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox vault read -field=password secret/dsp/accts/artifactory/dsdejenkins) core/src/bin/publishRelease.sh
```

You can view what is in the artifactory here: https://broadinstitute.jfrog.io/broadinstitute/webapp/#/home

After publishing, update [model/CHANGELOG.md](model/CHANGELOG.md) properly.
