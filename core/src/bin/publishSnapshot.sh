#!/usr/bin/env bash

set -e

SBT_OPTS=-Dproject.isSnapshot=true sbt +publish
