# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands
- Build: `mvn clean package`
- Run tests: `mvn test`
- Run single test: `mvn test -Dtest=TestClassName` or `mvn test -Dtest=TestClassName#testMethodName`
- Run service: `java -Ddp.config=path/to/config.yml -Dlog4j.configurationFile=path/to/log4j2.xml -cp target/dp-service-1.10.0-shaded.jar com.ospreydcs.dp.service.<service>.server.<Service>GrpcServer`

## Code Style Guidelines
- Java 21 is used for this project
- MongoDB is used for persistence with standard document patterns
- Follow existing naming conventions (CamelCase for classes, lowerCamelCase for methods)
- Package structure: `com.ospreydcs.dp.service.<component>`
- Framework patterns: handlers, jobs, dispatchers for each API method
- Error handling uses DpException and appropriate logging
- Service components follow the handler/dispatcher pattern described in documentation
- Thorough test coverage is expected for all new code
- Follow existing code patterns for MongoDB document serialization/deserialization