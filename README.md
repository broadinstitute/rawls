[![Build Status](https://travis-ci.org/broadinstitute/rawls.svg?branch=master)](https://travis-ci.org/broadinstitute/rawls) [![Coverage Status](https://coveralls.io/repos/broadinstitute/rawls/badge.svg?branch=master)](https://coveralls.io/r/broadinstitute/rawls?branch=master)

#DSDE Workspace Service

##Getting started
```
$ git clone https://github.com/broadinstitute/rawls.git
$ cd rawls
$ sbt clean compile test
```

Running a local server (requires config settings to be defined in ```/etc/rawls.conf```):

```
$ sbt assembly
$ java -jar target/scala-*/rawls-assembly-*.jar
> (... should be running at localhost:8080 ...)
```

See the wiki for detailed documentation.

##Developer quick links:
* Swagger UI: https://rawls-dev.broadinstitute.org
* OrientDB UI: http://orientdb-dev.broadinstitute.org:2480
* Jenkins: https://dsde-jenkins.broadinstitute.org/job/rawls-dev-build
