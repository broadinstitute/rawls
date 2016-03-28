#!/bin/bash

set -e

exec java $JAVA_OPTS -agentpath:/jprofiler9/bin/linux-x64/libjprofilerti.so=port=8849,nowait -jar $(find /rawls -name 'rawls*.jar')
