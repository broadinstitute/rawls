#!/usr/bin/env bash

set -e

sbt -J-Dproject.isSnapshot=true +publish
