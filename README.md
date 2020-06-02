[![Build Status](https://img.shields.io/travis/com/broadinstitute/rawls)](https://travis-ci.com/broadinstitute/rawls?branch=develop)
[![Coverage Status](https://img.shields.io/codecov/c/gh/broadinstitute/rawls)](https://codecov.io/gh/broadinstitute/rawls)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-green)](https://github.com/broadinstitute/rawls/blob/master/LICENSE.txt)

# rawls

1. The workspace manager for the Prometheus project
2. Bill Rawls, Deputy Commissioner for Operations from *The Wire*:

![](http://vignette2.wikia.nocookie.net/thewire/images/b/b5/Rawls.jpg)

## Getting started
```
$ git clone git@github.com:broadinstitute/rawls.git
$ brew install git-secrets # if not already installed
$ cd rawls
$ sbt antlr4:antlr4Generate # Generates source code for IntellIJ IDEA
$ cp -r hooks/* .git/hooks  #this step can be skipped if you use the rsync script to spin up locally
$ chmod 755 .git/hooks/apply-git-secrets.sh #this step as well
$ sbt clean compile test
```

See the wiki for detailed documentation.


## Developer quick links:
* Swagger UI: https://rawls.dsde-dev.broadinstitute.org
* Jenkins: https://dsde-jenkins.broadinstitute.org/job/rawls-dev-build
* Running locally in docker https://github.com/broadinstitute/firecloud-develop

## Unit Testing with MySQL in Docker
Ensure that docker is up to date and initialized.
Spin up mysql locally and validate that it is working:
```
./docker/run-mysql.sh start
```
Run tests.
```
export SBT_OPTS="-Xmx2G -Xms1G -Dmysql.host=localhost -Dmysql.port=3310 -Ddirectory.url=ldap://localhost:3389 -Ddirectory.password=testtesttest -Dcom.sun.jndi.ldap.connect.pool.maxsize=100"
sbt clean compile test
```
And when you're done, spin down mysql (it is also fine to leave it running for your next round of tests):
```
./docker/run-mysql.sh stop
```

## Build Rawls docker image
Build Rawls jar
```
./docker/build.sh jar
```

Build Rawls jar and docker image
```
./docker/build.sh jar -d build
```
