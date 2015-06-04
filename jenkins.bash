#!/bin/bash
#
# jenkins build script for flock

set -xueo pipefail

# need to use java7
export JAVA_HOME=/usr/lib/jvm/java-7-oracle
# for lein to use java7
export JAVA_CMD=${JAVA_HOME}/bin/java


cd ${WORKSPACE}/lib/
lein install

cd ${WORKSPACE}/sql
cat flock-schema.sql | mysql -u flock -h localhost --password=flock flockbuilder

cd ${WORKSPACE}/


lein midje

lein uberjar

