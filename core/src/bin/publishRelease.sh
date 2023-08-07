#!/usr/bin/env bash

set -e

sbt -J-Dproject.isSnapshot=false +publish
