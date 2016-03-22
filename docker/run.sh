#!/bin/bash

set -e

exec java $JAVA_OPTS -jar $(find /rawls -name 'rawls*.jar')
