package io.confluent.healthcheck;

public interface KafkaHealthCheckMBean {
    boolean isHealthy();
}