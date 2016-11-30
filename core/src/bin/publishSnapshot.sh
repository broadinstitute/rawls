#!/usr/bin/env bash

set -e

sbt rawlsModel:publish -Dproject.isSnapshot=true
