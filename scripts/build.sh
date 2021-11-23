#!/usr/bin/env bash

set -ex

echo "Releasing Function Mesh Worker Service"

version=${1#v}
if [[ "x$version" == "x" ]]; then
  echo "You need give a version number of the Function Mesh Worker Service"
  exit 1
fi

# Create a direcotry to save assets
ASSETS_DIR=release
mkdir $ASSETS_DIR

./scripts/generate-crd.sh
mvn license:format
mvn clean install -DskipTests
mv target/mesh-worker-service-*.nar  ./$ASSETS_DIR
cp README.md ./$ASSETS_DIR/mesh-worker-service-readme.md
