

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
```

## scalafmt

To format files, scalafmt can be [set up to run from Intellij](https://scalameta.org/scalafmt/docs/installation.html#intellij)

When a PR is opened, scalafmt will check formatting for modified files.

To run scalafmt from the commandline:

```shell
sbt scalafmt
```

## Unit Testing with MySQL in Docker
Ensure that docker is up to date and initialized.
Spin up mysql locally and validate that it is working:

```sh
./docker/run-mysql.sh start
```

Run tests.

```sh
export JAVA_OPTS="-Xmx2G -Xms1G -Dmysql.host=localhost -Dmysql.port=3310"
sbt clean compile test
```

And when you're done, spin down mysql (it is also fine to leave it running for your next round of tests):

```sh
./docker/run-mysql.sh stop
```

## Running Locally

### Requirements:

* [Docker Desktop](https://www.docker.com/products/docker-desktop) (4GB+, 8GB recommended)
* Broad internal internet connection (or VPN, non-split recommended)
* Render the local configuration files. From the root of the [firecloud-develop](https://github.com/broadinstitute/firecloud-develop) repo, run:
```sh
sh run-context/local/scripts/firecloud-setup.sh
```
Note: this script will offer to set up configuration for several other services as well. You can skip those if you only want to set up configuration for Rawls. If this is your first time running Rawls or rendering configuration files, you will want to run through the "Setup vault" step. You must also have a Ruby interpreter installed at `/usr/bin/ruby` for this script to work as expected.

*  The `/etc/hosts` file on your machine must contain this entry (for calling Rawls endpoints):
```sh
127.0.0.1	local.broadinstitute.org
```

### Running:

After satisfying the above requirements, execute the following command from the root of the Rawls repo:

```sh
./config/docker-rsync-local-rawls.sh
```

By default, this will set up an instance of rawls pointing to the database and Sam in dev. 
It will also set up a process that will watch the local files for changes, and restart the service when source files change.

See docker-rsync-local-rawls.sh for more configuration options.

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

Supported Scala versions: 2.13

Running the `publishRelease.sh` script publishes a release of rawls-model, workbench-util and workbench-google to Artifactory. You should do this manually from the base directory of the repo when you change something in `model/src`, `util/src` or `google/src`.
- [Jenkins runs `publishSnapshot.sh` on every dev build](https://fc-jenkins.dsp-techops.broadinstitute.org/job/rawls-build/), but that makes "unofficial" `-SNAP` versions.

To publish a temporary or test version, use `publishSnapshot.sh` like so:

```sh
VAULT_TOKEN=$(cat ~/.vault-token) ARTIFACTORY_USERNAME=dsdejenkins ARTIFACTORY_PASSWORD=$(docker run -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox vault read -field=password secret/dsp/accts/artifactory/dsdejenkins) core/src/bin/publishSnapshot.sh
```

To publish an official release, you can run the following command:

```sh
VAULT_TOKEN=$(cat ~/.vault-token) ARTIFACTORY_USERNAME=dsdejenkins ARTIFACTORY_PASSWORD=$(docker run -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox vault read -field=password secret/dsp/accts/artifactory/dsdejenkins) core/src/bin/publishRelease.sh
```

You can view what is in the artifactory here: https://broadinstitute.jfrog.io/broadinstitute/webapp/#/home

After publishing:
* update [model/CHANGELOG.md](model/CHANGELOG.md) properly
* update the rawls-model dependency in the automation subdirectory, and ensure that sbt project is healthy
* update the rawls-model dependency in workbench-libs serviceTest, and ensure that sbt project is healthy


## Troubleshooting

If you get this error message: `java.lang.IllegalArgumentException: invalid flag: --release`:
* Run `java -version` and verify that you're running jdk11. If not, you will need to install / update your PATH.

For integration test issues, see [automation/README.md](automation/README.md).


## Debugging in Intellij IDEA
You can attach Intellij's interactive debugger to Rawls running locally in a 
docker container configured via `run-context/local/scripts/firecloud-setup.sh` in 
[firecloud-develop](https://github.com/broadinstitute/firecloud-develop/blob/dev/run-context/local/README.md).

Add a "Remote JVM Debug" configuration that attaches to `localhost` on port `25050`.
See the link below for more detailed steps.
https://blog.jetbrains.com/idea/2019/04/debug-your-java-applications-in-docker-using-intellij-idea/
