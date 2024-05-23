package io.confluent.healthcheck;

public class KafkaHealthCheck implements KafkaHealthCheckMBean {
    private boolean healthy;

    public KafkaHealthCheck() {
        this.healthy = true; // 初始狀態設置為健康
    }

    @Override
    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }
}