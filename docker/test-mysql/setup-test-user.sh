#!/usr/bin/env bash

# inspired by / copied from https://github.com/nkratzke/EasyMySQL

# This script starts the database server.
echo "Creating user $user"

# Now the provided user credentials are added
/usr/sbin/mysqld &
sleep 5
echo "CREATE USER '$user' IDENTIFIED BY '$password'" | mysql --default-character-set=utf8
echo "GRANT ALL PRIVILEGES ON *.* TO '$user'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES" | mysql --default-character-set=utf8
echo "finished"
