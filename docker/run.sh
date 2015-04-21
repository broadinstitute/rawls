#!/bin/bash

set -e

java -jar $(find /rawls | grep 'rawls.*\.jar')
