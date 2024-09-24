

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
cd rawls
sbt antlr4:antlr4Generate # Generates source code for IntellIJ IDEA
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
* Make sure you have `kubectl` and `gcloud` installed.
* You will then need to authenticated in gcloud; if you are not already then running the script will ask you to.
* Render the local configuration files. From the root of the Rawls repo, run:
```sh
./local-dev/bin/render
```
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

**Important**: It is highly recommended that use your own Cloud SQL instance when running an instance of back Rawls. See note below on database work.

#### Developing Database Schema Changes

If you are writing Liquibase migrations or doing database work, it is mandatory that you create your own Cloud SQL instance and develop against that. To point to your own CloudSQL instance, modify `/config/sqlproxy.env` accordingly.



## Developer quick links:
* Swagger UI: https://rawls.dsde-dev.broadinstitute.org

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

Running the `publishRelease.sh` script publishes a release of rawls-model, workbench-util and workbench-google to Artifactory.
You should do this manually from the base directory of the repo when you change something in `model/src`, `util/src` or `google/src`.

- The [`rawls-build` GitHub action](https://github.com/broadinstitute/terra-github-workflows/actions/workflows/rawls-build.yaml)
publishes these libraries, but it makes "unofficial" `-SNAP` versions. This action runs on every dev build as part of the
[`rawls-build-tag-publish-and-run-tests` workflow](https://github.com/broadinstitute/rawls/blob/develop/.github/workflows/rawls-build-tag-publish-and-run-tests.yaml).


To publish a temporary or test version, use `publishSnapshot.sh` like so:

```sh
VAULT_TOKEN=$(cat ~/.vault-token) ARTIFACTORY_USERNAME=dsdejenkins ARTIFACTORY_PASSWORD=$(docker run -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:dev vault read -field=password secret/dsp/accts/artifactory/dsdejenkins) core/src/bin/publishSnapshot.sh
```

To publish an official release, you can run the following command:

```sh
VAULT_TOKEN=$(cat ~/.vault-token) ARTIFACTORY_USERNAME=dsdejenkins ARTIFACTORY_PASSWORD=$(docker run -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:dev vault read -field=password secret/dsp/accts/artifactory/dsdejenkins) core/src/bin/publishRelease.sh
```

You can view what is in the artifactory here: https://broadinstitute.jfrog.io/broadinstitute/webapp/#/home

After publishing:
* update [model/CHANGELOG.md](model/CHANGELOG.md) properly
* update the rawls-model dependency in the automation subdirectory, and ensure that sbt project is healthy
* update the rawls-model dependency in workbench-libs serviceTest, and ensure that sbt project is healthy


## Troubleshooting

If you get the error message `release version 17 not supported`:
* Run `java -version` and verify that you're running 17. If not, you will need to install / update your PATH.

If you have trouble submitting workflows and see errors like `HTTP error calling URI https://cromiam-priv.dsde-dev.broadinstitute.org`:
* Connect to the NonSplit VPN and try again
* CromIAM doesn't accept requests from outside the Broad trusted IP space 

When running back Rawls with a DB clone, the app may crash on launch with an error related to `OpenTelemetry`.
* Work around by setting `entityStatisticsCache.enabled = false`.

For integration test issues, see [automation/README.md](automation/README.md).


## Debugging in Intellij IDEA
You can attach Intellij's interactive debugger to Rawls running locally in a 
docker container run via the `./config/docker-rsync-local-rawls.sh` script.

Add a "Remote JVM Debug" configuration that attaches to `localhost` on port `25050`.
See the link below for more detailed steps.
https://blog.jetbrains.com/idea/2019/04/debug-your-java-applications-in-docker-using-intellij-idea/
