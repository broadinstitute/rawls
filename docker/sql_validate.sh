#!/bin/bash

# validate mysql
sleep 60
mysql -uroot -prawls-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -urawls-test -prawls-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -urawls-test -prawls-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()" testdb