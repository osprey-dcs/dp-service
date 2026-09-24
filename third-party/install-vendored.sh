#!/bin/sh
# Install every vendored artifact under third-party/ into the local Maven repository.
#
# These artifacts are not on Maven Central and their only public host is maven.scijava.org,
# which has served 503 for JAR downloads. Every build path that builds dp-service runs this
# first: ci.yml, release.yml, release-image.yml, and the Dockerfile's builder stage. A new
# build path needs it too. See third-party/cisd-jhdf5/README.md before removing anything here.
#
# Run from the repository root.
set -eu

install_vendored() {
    mvn -B -q install:install-file -Dfile="$1.jar" -DpomFile="$1.pom"
    echo "installed $1"
}

install_vendored third-party/cisd-base/base-18.09.0
install_vendored third-party/cisd-jhdf5/jhdf5-19.04.1
