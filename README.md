1. Create a JMX Exporter configuration file

Create or modify the JMX Exporter's configuration file (for example, jmx_exporter_config.yml) and add a rule to collect the io.confluent.healthcheck:type=KafkaHealthCheck metric:

```yaml
startDelaySeconds: 0
hostPort: 0.0.0.0:12345
jmxUrl: service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi
ssl: false
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  - pattern: "io.confluent.healthcheck<type=KafkaHealthCheck><>(isHealthy)"
    name: "kafka_healthcheck_is_healthy"
    type: GAUGE
    help: "Kafka cluster health check status"
```
2. Configure Kafka to use JMX Exporter
Modify the Kafka startup script (for example, kafka-server-start.sh) and add the JVM parameters of the JMX Exporter:
```yaml
export KAFKA_OPTS="$KAFKA_OPTS -javaagent:/path/to/jmx_prometheus_javaagent.jar=12345:/path/to/jmx_exporter_config.yml"

```

How to build :

git clone https://github.com/Monohitoxx/cp-health-check.git

```sh
./gradlew build shadowJar

```
