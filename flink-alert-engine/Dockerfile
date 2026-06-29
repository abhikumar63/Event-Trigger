FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline

COPY src ./src
RUN mvn package -DskipTests

FROM flink:1.17-java11
WORKDIR /app
COPY --from=builder /app/target/flink-alert-engine-1.0-SNAPSHOT.jar /app/job.jar

# Install curl for checking JobManager health
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Wait for JobManager and submit the job
CMD bash -c "\
    echo 'Waiting for JobManager to be ready on jobmanager:8081...' && \
    until curl -s http://jobmanager:8081/config; do \
        sleep 2; \
    done; \
    echo 'JobManager is up. Waiting an extra 5 seconds...' && \
    sleep 5; \
    echo 'Submitting Flink job...' && \
    flink run -m jobmanager:8081 -d /app/job.jar && \
    echo 'Job submitted! Keeping container alive for logs.' && \
    tail -f /dev/null \
"
