FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build

# Build jar from source to avoid stale host-side target artifacts
COPY pom.xml .
COPY src ./src
RUN mvn -DskipTests clean package

FROM flink:1.17.2-scala_2.12
COPY --from=build /build/target/flink-kafka-print-0.1.0.jar /opt/flink/usrlib/flink-kafka-print-0.1.0.jar
