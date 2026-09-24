# Build stage: clone dp-grpc, install it to local maven repo, then build this project
ARG DP_GRPC_REPO=https://github.com/osprey-dcs/dp-grpc.git
ARG DP_GRPC_REF=main

FROM maven:3.9.6-eclipse-temurin-21 AS builder

# install git (Debian-based image)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG DP_GRPC_REPO
ARG DP_GRPC_REF
WORKDIR /build

# Fetch dp-grpc at DP_GRPC_REF and install it to the local maven repo so this project's dependency
# resolves.  DP_GRPC_REF may be a branch, a tag, or a commit SHA (release-image.yml passes the
# commit its test job built against).  There is deliberately NO fallback: an unresolvable ref fails
# the build rather than silently building dp-grpc's default branch, which a signed image would then
# vouch for without knowing it (#221).  `git clone --branch` cannot take a SHA, hence fetch.
RUN git init -q dp-grpc \
    && git -C dp-grpc fetch --depth 1 "${DP_GRPC_REPO}" "${DP_GRPC_REF}" \
    && git -C dp-grpc checkout -q FETCH_HEAD \
    && echo "dp-grpc ${DP_GRPC_REF} -> $(git -C dp-grpc rev-parse HEAD)"
RUN mvn -f dp-grpc/pom.xml -B -DskipTests install

# Copy current project sources into the image and build
COPY . /build/app
WORKDIR /build/app
# cisd:jhdf5 and cisd:base are not on Maven Central and their only host, maven.scijava.org, has
# served 503 for JARs. Install the vendored copies (already in the build context) so the image
# build does not depend on that host. See third-party/cisd-jhdf5/README.md before removing this.
RUN sh third-party/install-vendored.sh
RUN mvn -B -DskipTests package

# Normalize artifact name: copy first shaded jar or first jar into a known location
RUN mkdir -p /build/artifact \
    && JAR=$(ls target/*-shaded.jar 2>/dev/null || ls target/*.jar 2>/dev/null | grep -v "-sources\|-javadoc" | head -n 1) \
    && if [ -z "$JAR" ]; then echo "No jar found in target/" >&2; exit 1; fi \
    && cp "$JAR" /build/artifact/app.jar

# Runtime image
FROM eclipse-temurin:21-jre
LABEL maintainer="dp-service CI"

WORKDIR /app
COPY --from=builder /build/artifact/app.jar /app/app.jar

# The gRPC port and the Prometheus metrics port (issue #212) of each service. Which pair is
# actually served depends on the class this container runs: the jar's Main-Class is
# IngestionGrpcServer, so a plain `java -jar` starts ingestion, and the other three services are
# started by overriding the entrypoint, e.g.
#
#   java -cp /app/app.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer
#
# EXPOSE documents the image's ports; it does not publish them. Kubernetes reaches a containerPort
# whether or not it is declared here, so this list is for the operator writing that manifest.
# Metrics are on by default and the service FAILS TO START if its metrics port cannot be bound
# (set DP_TELEMETRY_ENABLED=false to disable). See doc/metrics.md.
#
#   ingestion 50051/9464   query 50052/9465   annotation 50053/9466   ingestion-stream 50054/9467
EXPOSE 50051 50052 50053 50054 9464 9465 9466 9467
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
