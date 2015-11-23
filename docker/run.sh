#!/bin/bash

set -e

rawlsjar=$(find /rawls -name 'rawls*.jar')
exec java -Djava.library.path=./native -javaagent:$rawlsjar -jar $rawlsjar server
