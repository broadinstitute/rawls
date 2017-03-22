#!/bin/bash

# validate mysql
echo "sleeping for 60 seconds during mysql boot..."
sleep 60
mysql -uroot -prawls-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -urawls-test -prawls-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -urawls-test -prawls-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()" testdb