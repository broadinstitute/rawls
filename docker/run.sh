#!/bin/bash

set -e

exec java -jar $(find /rawls -name 'rawls*.jar')
