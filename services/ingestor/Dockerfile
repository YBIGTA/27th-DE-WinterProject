# Build stage
FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY src ./src
RUN gradle build -x test --no-daemon

# Runtime stage
FROM eclipse-temurin:17-jre
WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Add non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser
# Ensure runtime directories are writable by appuser
RUN mkdir -p /app/data && chown -R appuser:appgroup /app

COPY --from=build --chown=appuser:appgroup /app/build/libs/*.jar app.jar

USER appuser

# Default environment variables
ENV SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV SERVER_PORT=8080
ENV APP_DLQ_FILEPATH=/app/data/dead_letter_queue.jsonl

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
