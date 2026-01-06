#!/bin/bash
mvn package -D skipTests

# Copy to dp-support
cp target/dp-service-*-shaded.jar ../../dp-support/lib/dp-service.jar
echo "JAR copied to dp-support/lib/"

# Copy to other locations as needed
# cp target/dp-service-*-shaded.jar ../some-other-project/lib/dp-service.jar
# cp target/dp-service-*-shaded.jar /path/to/deployment/staging/lib/dp-service.jar
# echo "All JAR distributions updated"
