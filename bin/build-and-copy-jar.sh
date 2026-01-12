#!/bin/bash
mvn package -D skipTests

# Copy to dp-support
cp target/dp-service-*-shaded.jar ../../data-platform/lib/dp-service.jar
echo "JAR copied to dp-support/lib/"

