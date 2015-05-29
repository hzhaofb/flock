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

cat flock-schema.sql | mysql -u flock -h localhost --password=flock flocklogbuilder

cd ${WORKSPACE}/

git rev-parse HEAD > resources/git-hash.txt

rm -f memcached.pid
echo "ensure memcache is running"
if [ "$(pgrep memcached)" != "" ]; then
    echo "memcached is running"
else
    memcached -P memcached.pid &
fi

lein midje

lein uberjar

if [ -f "memcached.pid" ]; then
  kill -9 `cat memcached.pid`
  rm -f memcached.pid
fi

gzip -c target/uberjar/flock-standalone.jar > flock-${GIT_COMMIT}.jar.gz
