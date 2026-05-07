# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

# Download dependencies first (cached unless pom.xml changes)
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Build the fat JAR
COPY src ./src
RUN mvn -q package -DskipTests

# Download New Relic Java agent (9.1.0) in the build stage which has curl/unzip
RUN apt-get update -qq && apt-get install -y --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL \
    "https://download.newrelic.com/newrelic/java-agent/newrelic-agent/9.1.0/newrelic-java-9.1.0.zip" \
    -o /tmp/newrelic.zip \
    && unzip -jo /tmp/newrelic.zip "newrelic/newrelic.jar" -d /app/newrelic \
    && rm /tmp/newrelic.zip

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /app/target/kafka-newrelic-app.jar app.jar
COPY --from=build /app/newrelic/newrelic.jar         newrelic/newrelic.jar

# newrelic.yml is mounted at runtime via docker-compose volume
ENTRYPOINT ["java", \
    "-javaagent:/app/newrelic/newrelic.jar", \
    "-Dnewrelic.config.file=/app/newrelic.yml", \
    "-Xms256m", "-Xmx512m", \
    "-XX:+UseG1GC", \
    "-jar", "/app/app.jar"]
