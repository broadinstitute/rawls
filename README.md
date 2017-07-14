[![Build Status](https://travis-ci.org/broadinstitute/rawls.svg?branch=develop)](https://travis-ci.org/broadinstitute/rawls) [![Coverage Status](https://coveralls.io/repos/broadinstitute/rawls/badge.svg?branch=develop)](https://coveralls.io/r/broadinstitute/rawls?branch=develop)

# rawls

1. The workspace manager for the Prometheus project
2. Bill Rawls, Deputy Commissioner for Operations from *The Wire*:

![](http://vignette2.wikia.nocookie.net/thewire/images/b/b5/Rawls.jpg)

## Getting started
```
$ git clone https://github.com/broadinstitute/rawls.git
$ cd rawls
$ sbt clean compile test
```

See the wiki for detailed documentation.


## Developer quick links:
* Swagger UI: https://rawls-dev.broadinstitute.org
* Jenkins: https://dsde-jenkins.broadinstitute.org/job/rawls-dev-build
* Running locally in docker https://github.com/broadinstitute/firecloud-develop

## Unit Testing with MySQL in Docker
Ensure that docker is up to date and initialized.
Spin up mysql locally and validate that it is working:
```
./docker/run-mysql.sh
```
Run tests.
```
sbt clean compile test -Dmysql.host=localhost -Dmysql.port=3310
```
And when you're done, spin down mysql:
```
docker stop mysql && docker rm mysql
```

## Integration Testing with MySQL in Docker
Running the Integration Test requires the above setup plus a few support files in the */etc* folder.
These can be softlinks to existing files in your Rawls Config folder.  If you have set that up correctly
via the procedure described in the firecloud-develop repo, run these commands:
```
cd /etc
sudo ln -s <path_to_rawls_src>/config/rawls.conf
sudo ln -s <path_to_rawls_src>/config/rawls-account.pem
sudo ln -s <path_to_rawls_src>/config/billing-account.pem
```
Run tests using mysql similarly to unit tests.
```
sbt clean compile it:test -Dmysql.host=localhost -Dmysql.port=3310
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
