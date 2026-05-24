FROM eclipse-temurin:17-jre-alpine

ARG KAFKA_VERSION=3.7.1
ARG SCALA_VERSION=2.13

RUN apk add --no-cache bash curl

RUN curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    | tar -xz -C /opt \
    && mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

ENV PATH="/opt/kafka/bin:$PATH"
ENV KAFKA_LOG_DIRS=/var/kafka/data

RUN mkdir -p /var/kafka/data

COPY server.properties /opt/kafka/config/kraft/server.properties
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9092

ENTRYPOINT ["/entrypoint.sh"]
