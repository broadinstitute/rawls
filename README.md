[![Build Status](https://travis-ci.org/broadinstitute/rawls.svg?branch=master)](https://travis-ci.org/broadinstitute/rawls) [![Coverage Status](https://coveralls.io/repos/broadinstitute/rawls/badge.svg?branch=master)](https://coveralls.io/r/broadinstitute/rawls?branch=master)

#rawls

1. The workspace manager for the Prometheus project
2. Bill Rawls, Deputy Commissioner for Operations from *The Wire*:

![](http://vignette2.wikia.nocookie.net/thewire/images/b/b5/Rawls.jpg)

##Getting started
```
$ git clone https://github.com/broadinstitute/rawls.git
$ cd rawls
$ sbt clean compile test
```

See the wiki for detailed documentation.

##Developer quick links:
* Swagger UI: https://rawls-dev.broadinstitute.org
* Jenkins: https://dsde-jenkins.broadinstitute.org/job/rawls-dev-build
* Running locally in docker https://github.com/broadinstitute/firecloud-develop

##Unit Testing in Docker with MySQL

###Docker Builds
Check to see that you have a Dev Base image for Rawls:
```
$ docker pull broadinstitute/rawls:dev-base
```

If this fails, it is necessary to build this image, which may take a long time (~20 minutes).
```
$ docker build -t broadinstitute/rawls:dev-base ${RAWLS_SRC_DIR}
```

When that is done, build a Test MySQL image:
```
$ docker build -t broadinstitute/rawls:test-mysql ${RAWLS_SRC_DIR}/docker/test-mysql
```

###Docker Run
Run docker with the following arguments to ensure your local Rawls source is mounted inside the container.
```
$ docker run -d --name rawls-test -v ${RAWLS_SRC_DIR}:/app:rw broadinstitute/rawls:test-mysql
```

Enter the container:
```
docker exec -it rawls-test bash
```

Run the unit tests:
```
# cd /app
# sbt -Dtestdb=mysql test
```
