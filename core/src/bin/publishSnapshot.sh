#!/usr/bin/env bash

set -e

sbt +publish -Dproject.isSnapshot=true
