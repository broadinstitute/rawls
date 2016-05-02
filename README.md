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

## Unit Testing with MySQL in Docker 
Ensure that docker is up to date and initialized.
Spin up mysql locally:
```
docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD=rawls-test -e MYSQL_USER=rawls-test -e MYSQL_PASSWORD=rawls-test -e MYSQL_DATABASE=testdb -d mysql/mysql-server:5.7
```
Run tests. Replace the `default` value with your docker machine name (or leave it out altogether if you have only a single machine):
```
sbt clean compile test -Dmysql.host=`docker-machine ip default`
```
And when you're done, spin down mysql:
```
docker stop mysql && docker rm mysql
```

## Unit Testing
Unit tests require a mysql host configured similarly to the above docker instance.  
```
sbt clean compile test -Dmysql.host=<mysql hostname>
```

Unit tests can optionally include a custom mysql port. 
The default is 3306 but can be changed with an optional environment variable:
```
sbt clean compile test -Dmysql.host=<mysql hostname> -Dmysql.port=<mysql port>
```
