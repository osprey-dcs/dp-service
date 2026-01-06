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

# Clone dp-grpc and install to local maven repo so this project's dependency resolves
RUN git clone --depth 1 --branch "${DP_GRPC_REF}" "${DP_GRPC_REPO}" dp-grpc || git clone "${DP_GRPC_REPO}" dp-grpc
RUN mvn -f dp-grpc/pom.xml -B -DskipTests install

# Copy current project sources into the image and build
COPY . /build/app
WORKDIR /build/app
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

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
